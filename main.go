// Command ac-chat-app serves a browser chat UI for agent-compose agents.
//
// The browser cannot hold a conversation with the daemon directly: Connect
// needs HTTP/2 bidirectional streaming for a multi-turn session, and a browser
// fetch() with a streaming request body is half duplex, so nothing comes back
// while that body stays open. This app keeps the conversation on the Go side,
// where the chat SDK handles it, and gives the browser the one-way event
// stream it can actually consume.
package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/chaitin/agent-compose/sdk/go/chat"
)

//go:embed public/index.html
var assets embed.FS

func main() {
	// Settings resolve flag first, then the environment, then .env, then a
	// default. The file is read before the flags are defined so that it only
	// supplies defaults: anything given on the command line still wins.
	if err := loadEnvFile(envFile); err != nil {
		log.Fatalf("read %s: %v", envFile, err)
	}
	listen := flag.String("listen", settingOr("AC_LISTEN", "127.0.0.1:7500"), "address to serve the UI on")
	daemon := flag.String("daemon", settingOr("AC_DAEMON", "http://127.0.0.1:7411"), "agent-compose HTTP address")
	origins := flag.String("allow-origin", settingOr("AC_ALLOW_ORIGIN", ""), "comma-separated extra origins allowed to open a chat socket")
	state := flag.String("state", settingOr("AC_STATE", defaultStatePath()), "state file holding accounts and the chat list")
	idleAfter := flag.Duration("idle-after", 15*time.Minute,
		"end a conversation's run once nobody has watched it for this long; its next message resumes the same sandbox")
	sweepEvery := flag.Duration("sweep-every", time.Minute, "how often to look for conversations that have gone idle")
	addUser := flag.String("add-user", "", "create an account with this name, then exit")
	setPassword := flag.String("set-password", "", "change this account's password, then exit")
	flag.Parse()

	backing, err := openStore(*state)
	if err != nil {
		log.Fatal(err)
	}
	if *addUser != "" || *setPassword != "" {
		if err := manageAccount(backing, *addUser, *setPassword); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := bootstrap(backing); err != nil {
		log.Fatal(err)
	}

	token := daemonToken()
	if token == "" {
		log.Printf("no daemon token set; set AGENT_COMPOSE_AUTH_TOKEN if %s requires one", *daemon)
	}
	client, err := chat.New(chat.Config{
		BaseURL:   *daemon,
		Token:     chat.StaticToken(token),
		UserAgent: "ac-chat-app",
	})
	if err != nil {
		log.Fatal(err)
	}
	auth := newAuthenticator(backing)
	server := &uiServer{
		client:   client,
		daemon:   *daemon,
		token:    token,
		store:    backing,
		auth:     auth,
		sessions: map[string]*session{},
		upgrader: websocket.Upgrader{CheckOrigin: sameOriginOnly(strings.Split(*origins, ","))},
	}
	// Shutting down has to end the sessions this process is holding. The
	// daemon keeps a run alive when its client disconnects and has nothing
	// that expires it, so a process that simply exits leaves one running —
	// and one sandbox — per conversation it had open.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go server.release(ctx, *sweepEvery, *idleAfter)
	go auth.sweep(*sweepEvery)

	httpServer := &http.Server{Addr: *listen, Handler: server.routes()}
	go func() {
		<-ctx.Done()
		stop() // Restore default handling, so a second signal kills us outright.
		log.Printf("shutting down; releasing open conversations")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		// Stop serving before releasing, so nothing re-attaches on the way out.
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		server.closeAll(shutdownCtx)
	}()

	log.Printf("chat UI on http://%s (daemon %s, state %s)", *listen, *daemon, *state)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-ctx.Done()
}

// shutdownTimeout bounds the whole exit: draining requests and then ending the
// conversations this process holds.
const shutdownTimeout = 45 * time.Second

// manageAccount runs the two account commands, which exist so a password never
// has to be typed on a command line where the shell history keeps it.
func manageAccount(backing *store, add, reset string) error {
	if add != "" {
		password, err := readPassword("password for " + add + ": ")
		if err != nil {
			return err
		}
		if err := backing.addUser(add, password); err != nil {
			return err
		}
		log.Printf("created %s", add)
		return nil
	}
	password, err := readPassword("new password for " + reset + ": ")
	if err != nil {
		return err
	}
	if err := backing.setPassword(reset, password); err != nil {
		return err
	}
	log.Printf("password changed for %s", reset)
	return nil
}

// bootstrap creates a first account when there is none, printing a generated
// password once. A UI with no accounts would otherwise be unreachable, and a
// fixed default password would be worse than none.
func bootstrap(backing *store) error {
	if backing.users() > 0 {
		return nil
	}
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	password := base64.RawURLEncoding.EncodeToString(raw)
	if err := backing.addUser("admin", password); err != nil {
		return err
	}
	log.Printf("no accounts yet — created admin with password %s", password)
	log.Printf("this password is not shown again; change it with -set-password admin")
	return nil
}

// readPassword takes a password off stdin without echoing it when stdin is a
// terminal, and reads a piped line otherwise so the command stays scriptable.
func readPassword(prompt string) (string, error) {
	_, _ = os.Stderr.WriteString(prompt)
	defer func() { _, _ = os.Stderr.WriteString("\n") }()
	password, err := readSecret(os.Stdin)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(password) == "" {
		return "", errors.New("password is required")
	}
	return password, nil
}

// defaultStatePath keeps state in this app's own directory. It is this app's
// state - accounts and chat titles - not the daemon's, and the daemon may well
// be on another host.
func defaultStatePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "ac-chat-app-state.json"
	}
	return filepath.Join(home, ".ac-chat-app", "state.json")
}

// daemonToken resolves the bearer token the daemon's control plane expects.
//
// The name is the daemon's own (AGENT_COMPOSE_AUTH_TOKEN, read in
// pkg/config/config.go), so one .env can configure both sides. AC_DAEMON_TOKEN
// is accepted as well for a deployment that would rather keep this app's
// settings under one prefix.
//
// An empty token is a daemon with authentication disabled, which is a
// supported configuration rather than a mistake.
func daemonToken() string {
	if token := strings.TrimSpace(os.Getenv("AGENT_COMPOSE_AUTH_TOKEN")); token != "" {
		return token
	}
	return strings.TrimSpace(os.Getenv("AC_DAEMON_TOKEN"))
}
