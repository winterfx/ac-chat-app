package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/chaitin/agent-compose/sdk/go/chat"
)

// newTestServer builds a server with two accounts in front of a daemon stub.
func newTestServer(t *testing.T, daemon *daemonStub) *uiServer {
	t.Helper()
	backing, err := openStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	for _, name := range []string{"alice", "bob"} {
		if err := backing.addUser(name, name+"-password"); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
	}
	client, err := chat.New(chat.Config{BaseURL: daemon.URL})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return &uiServer{
		client:   client,
		daemon:   daemon.URL,
		store:    backing,
		auth:     newAuthenticator(backing),
		sessions: map[string]*session{},
	}
}

// quietDaemon is a stub with no runs and no scripted turn, for the routes that
// never reach the agent.
func quietDaemon(t *testing.T) *daemonStub { return fakeDaemon(t, nil) }

// signIn returns the cookie a completed sign-in hands the browser.
func signIn(t *testing.T, server *uiServer, name string) *http.Cookie {
	t.Helper()
	body := strings.NewReader(`{"user":"` + name + `","password":"` + name + `-password"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/login", body)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("sign in as %s: %d %s", name, recorder.Code, recorder.Body)
	}
	for _, cookie := range recorder.Result().Cookies() {
		if cookie.Name == sessionCookie {
			return cookie
		}
	}
	t.Fatal("sign in returned no session cookie")
	return nil
}

func call(t *testing.T, server *uiServer, cookie *http.Cookie, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	request := httptest.NewRequest(method, path, reader)
	if cookie != nil {
		request.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	return recorder
}

func createConversation(t *testing.T, server *uiServer, cookie *http.Cookie) string {
	t.Helper()
	recorder := call(t, server, cookie, http.MethodPost, "/api/conversations",
		`{"projectId":"p","agentName":"a"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("create: %d %s", recorder.Code, recorder.Body)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode created: %v", err)
	}
	return created.ID
}

func TestEveryAPIRouteNeedsASession(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	guarded := []struct{ method, path string }{
		{http.MethodGet, "/api/agents"},
		{http.MethodGet, "/api/conversations"},
		{http.MethodPost, "/api/conversations"},
		{http.MethodGet, "/api/conversations/x"},
		{http.MethodPatch, "/api/conversations/x"},
		{http.MethodDelete, "/api/conversations/x"},
		{http.MethodGet, "/api/socket"},
		{http.MethodPost, "/api/conversations/recover"},
	}
	for _, route := range guarded {
		recorder := call(t, server, nil, route.method, route.path, "{}")
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s %s answered %d without a session, want 401", route.method, route.path, recorder.Code)
		}
	}
}

func TestThePageFontIsServedWithoutASession(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	page := call(t, server, nil, http.MethodGet, "/", "")
	found := regexp.MustCompile(`url\("(/fonts/[^"]+\.woff2)"\)`).FindStringSubmatch(page.Body.String())
	if found == nil {
		t.Fatal("the page declares no embedded font")
	}
	// The sign-in form is set in this font too, so it cannot wait for a session.
	font := call(t, server, nil, http.MethodGet, found[1], "")
	if font.Code != http.StatusOK {
		t.Fatalf("GET %s answered %d, want 200", found[1], font.Code)
	}
	if got := font.Header().Get("Content-Type"); got != "font/woff2" {
		t.Errorf("Content-Type %q, want font/woff2", got)
	}
	if !strings.HasPrefix(font.Body.String(), "wOF2") {
		t.Error("the served file is not a WOFF2 font")
	}
	if code := call(t, server, nil, http.MethodGet, "/fonts/OFL.txt", "").Code; code != http.StatusNotFound {
		t.Errorf("GET /fonts/OFL.txt answered %d; only font files are served", code)
	}
}

func TestAgentCatalogGroupsAgentsAndIdentifiesDaemon(t *testing.T) {
	daemon := quietDaemon(t)
	daemon.setProjectCatalog(
		[]map[string]any{{"projectId": "p1", "name": "代码项目"}},
		map[string][]map[string]any{
			"p1": {
				{"agentName": "coder", "displayName": "编码助手", "description": "修改代码", "enabled": true, "availability": "available"},
				{"agentName": "disabled", "enabled": false, "availability": "unavailable"},
			},
		},
	)
	server := newTestServer(t, daemon)
	cookie := signIn(t, server, "alice")
	recorder := call(t, server, cookie, http.MethodGet, "/api/agents", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("agent catalog: %d %s", recorder.Code, recorder.Body)
	}
	var response struct {
		Daemon   string `json:"daemon"`
		Projects []struct {
			ProjectID string `json:"projectId"`
			Name      string `json:"name"`
			Agents    []struct {
				AgentName   string `json:"agentName"`
				DisplayName string `json:"displayName"`
				Selectable  bool   `json:"selectable"`
			} `json:"agents"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode agent catalog: %v", err)
	}
	if response.Daemon != daemon.URL || len(response.Projects) != 1 {
		t.Fatalf("catalog project/daemon = %#v, want daemon %q and one project", response, daemon.URL)
	}
	project := response.Projects[0]
	if project.ProjectID != "p1" || project.Name != "代码项目" || len(project.Agents) != 2 {
		t.Fatalf("catalog project = %#v", project)
	}
	if project.Agents[0].AgentName != "coder" || project.Agents[0].DisplayName != "编码助手" || !project.Agents[0].Selectable {
		t.Fatalf("available agent = %#v", project.Agents[0])
	}
	if project.Agents[1].Selectable {
		t.Fatalf("disabled agent was selectable: %#v", project.Agents[1])
	}
}

// A conversation belongs to one person. Someone else asking for it is told it
// does not exist rather than that they may not have it, because the existence
// of another user's conversation is itself none of their business.
func TestAConversationIsInvisibleToAnyoneButItsOwner(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	alice, bob := signIn(t, server, "alice"), signIn(t, server, "bob")
	id := createConversation(t, server, alice)

	if recorder := call(t, server, alice, http.MethodGet, "/api/conversations/"+id, ""); recorder.Code != http.StatusOK {
		t.Fatalf("owner cannot read their own conversation: %d %s", recorder.Code, recorder.Body)
	}
	for _, attempt := range []struct{ method, path, body string }{
		{http.MethodGet, "/api/conversations/" + id, ""},
		{http.MethodPatch, "/api/conversations/" + id, `{"title":"mine now"}`},
		{http.MethodDelete, "/api/conversations/" + id, ""},
	} {
		recorder := call(t, server, bob, attempt.method, attempt.path, attempt.body)
		if recorder.Code != http.StatusNotFound {
			t.Errorf("%s by a stranger answered %d, want 404", attempt.method, recorder.Code)
		}
	}

	listed := call(t, server, bob, http.MethodGet, "/api/conversations", "")
	if strings.Contains(listed.Body.String(), id) {
		t.Errorf("another user's conversation appeared in the list: %s", listed.Body)
	}
}

func TestSignInRejectsTheWrongPasswordAndThenThrottles(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	for attempt := range maxSignInFailures {
		recorder := call(t, server, nil, http.MethodPost, "/api/login", `{"user":"alice","password":"nope"}`)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d answered %d, want 401", attempt, recorder.Code)
		}
	}
	// The right password is refused too once the address has been guessing:
	// throttling is about the source, not the credential.
	recorder := call(t, server, nil, http.MethodPost, "/api/login", `{"user":"alice","password":"alice-password"}`)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("answered %d after %d failures, want 429", recorder.Code, maxSignInFailures)
	}
}

func TestSigningOutInvalidatesTheCookie(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	alice := signIn(t, server, "alice")
	if recorder := call(t, server, alice, http.MethodPost, "/api/logout", ""); recorder.Code != http.StatusOK {
		t.Fatalf("sign out: %d %s", recorder.Code, recorder.Body)
	}
	if recorder := call(t, server, alice, http.MethodGet, "/api/conversations", ""); recorder.Code != http.StatusUnauthorized {
		t.Fatalf("the old cookie still worked: %d", recorder.Code)
	}
}

// A conversation nobody has written to yet has never run, so deleting it must
// not need the daemon: there is nothing on the daemon to delete.
func TestDeletingAnUnusedConversationDoesNotNeedTheDaemon(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	alice := signIn(t, server, "alice")
	id := createConversation(t, server, alice)

	recorder := call(t, server, alice, http.MethodDelete, "/api/conversations/"+id, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", recorder.Code, recorder.Body)
	}
	if strings.Contains(recorder.Body.String(), "warning") {
		t.Errorf("delete reached for the daemon: %s", recorder.Body)
	}
	if recorder := call(t, server, alice, http.MethodGet, "/api/conversations/"+id, ""); recorder.Code != http.StatusNotFound {
		t.Errorf("the conversation survived deletion: %d", recorder.Code)
	}
}

func TestTheChatListIsOrderedByLastUse(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	alice := signIn(t, server, "alice")
	first := createConversation(t, server, alice)
	second := createConversation(t, server, alice)

	if err := server.store.touch("alice", first, "回到第一个对话"); err != nil {
		t.Fatalf("touch: %v", err)
	}
	records := server.store.conversations("alice")
	if len(records) != 2 || records[0].ID != first {
		t.Fatalf("want the just-used conversation first, got %+v", records)
	}
	if records[0].Title != "回到第一个对话" {
		t.Errorf("first message did not name the conversation: %q", records[0].Title)
	}
	if !records[0].Started {
		t.Error("a conversation that received a message is not marked started")
	}
	if records[1].ID != second || records[1].Started {
		t.Errorf("the untouched conversation changed: %+v", records[1])
	}
}

func TestTheStateFileSurvivesARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")
	backing, err := openStore(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := backing.addUser("alice", "alice-password"); err != nil {
		t.Fatalf("add user: %v", err)
	}
	if _, err := backing.remember(conversationRecord{ID: "conv_1", Owner: "alice", Title: "旧对话"}); err != nil {
		t.Fatalf("remember: %v", err)
	}

	reopened, err := openStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.authenticate("alice", "alice-password"); err != nil {
		t.Errorf("the password did not survive a restart: %v", err)
	}
	if records := reopened.conversations("alice"); len(records) != 1 || records[0].Title != "旧对话" {
		t.Errorf("the chat list did not survive a restart: %+v", records)
	}
}

func TestAPasswordIsNeverStored(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	raw, err := json.Marshal(server.store.data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "alice-password") {
		t.Fatal("the state file contains a password in the clear")
	}
	verifier := server.store.data.Users["alice"]
	if verifier.matches("alice-passwore") {
		t.Error("a wrong password verified")
	}
	if !verifier.matches("alice-password") {
		t.Error("the right password did not verify")
	}
}

func TestTruncateDoesNotSplitARune(t *testing.T) {
	// "你好世界" is four three-byte runes; a cut at 7 bytes lands mid-rune.
	if got := truncate("你好世界", 7); got != "你好…" {
		t.Fatalf("truncate cut a rune in half: %q", got)
	}
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("truncate changed text that fits: %q", got)
	}
}

func TestTitlesComeFromTheFirstLineOfTheFirstMessage(t *testing.T) {
	if got := titleOf("  帮我看下这个 bug\n还有第二行  "); got != "帮我看下这个 bug" {
		t.Fatalf("title: %q", got)
	}
	if got := titleOf("   \n  "); got != "新对话" {
		t.Fatalf("an empty message should still name the conversation, got %q", got)
	}
}

// The UI groups a turn's work by the step an event names. An event that
// carries no step must not be given one, or the page would file it under a
// step it never belonged to.
func TestRenderedEventsCarryTheirStepAndOmitWhatWasNotReported(t *testing.T) {
	step := 2
	exit := int32(1)
	rendered := renderEvent(&chat.ToolCallEvent{
		Step: &step, ID: "call_1", Name: "bash", ToolKind: chat.ToolExecute,
		Status: chat.ToolCompleted, Command: "ls", ExitCode: &exit,
	})
	if rendered["step"] != 2 || rendered["exitCode"] != exit {
		t.Fatalf("tool call lost its step or exit code: %v", rendered)
	}
	if _, present := rendered["parentToolUseId"]; present {
		t.Errorf("an unreported field was sent anyway: %v", rendered)
	}

	unnumbered := renderEvent(&chat.TextDeltaEvent{Text: "hi"})
	if _, present := unnumbered["step"]; present {
		t.Errorf("an event with no step was given one: %v", unnumbered)
	}

	usage := renderEvent(&chat.UsageEvent{Scope: chat.ScopeTurn, InputTokens: 10, OutputTokens: 3})
	if usage["scope"] != string(chat.ScopeTurn) {
		t.Errorf("usage lost the scope that says whether it may be summed: %v", usage)
	}
	if _, present := usage["cachedTokens"]; present {
		t.Errorf("an unreported token count was sent as a zero: %v", usage)
	}

	runEnd := renderEvent(&chat.StepEndEvent{Scope: chat.StepEndScopeRun, StopReason: chat.StopEnd})
	if runEnd["scope"] != string(chat.StepEndScopeRun) || runEnd["stopReason"] != string(chat.StopEnd) {
		t.Errorf("run-level step end lost its scope: %v", runEnd)
	}
	if _, present := runEnd["step"]; present {
		t.Errorf("a run-level terminator was assigned a step: %v", runEnd)
	}
}

func TestRenderedEventsPreserveProviderExtensions(t *testing.T) {
	rendered := renderEvent(&chat.RawEvent{
		Name: "item.completed", Text: "done",
		PayloadJSON: `{"item":{"type":"agent_message","text":"done"}}`,
	})
	if rendered["kind"] != "item.completed" || rendered["name"] != "item.completed" || rendered["text"] != "done" {
		t.Fatalf("raw event fields were lost: %v", rendered)
	}
	payload, ok := rendered["payload"].(map[string]any)
	if !ok || payload["item"] == nil {
		t.Fatalf("raw event payload was lost: %v", rendered)
	}
}

func TestASessionExpires(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	alice := signIn(t, server, "alice")
	server.auth.mu.Lock()
	for key, found := range server.auth.sessions {
		found.expires = time.Now().Add(-time.Second)
		server.auth.sessions[key] = found
	}
	server.auth.mu.Unlock()

	if recorder := call(t, server, alice, http.MethodGet, "/api/conversations", ""); recorder.Code != http.StatusUnauthorized {
		t.Fatalf("an expired session still worked: %d", recorder.Code)
	}
}

// Losing the index does not lose the conversations: ownership lives in the
// chat.user label, so a conversation this server has no record of still
// resolves for its owner and still hides from everyone else. Rebuilding the
// list is an explicit request, because it costs a run detail read per run.
func TestAConversationResolvesWithoutALocalRecord(t *testing.T) {
	daemon := quietDaemon(t)
	daemon.listRuns(
		map[string]any{
			"runId": "run_a", "projectId": "p", "agentName": "a",
			"status": "RUN_STATUS_RUNNING", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
			"labels": map[string]string{
				"chat.conversation": "conv_alice", "chat.user": "alice", "chat.app": appName,
			},
		},
		map[string]any{
			"runId": "run_b", "projectId": "p", "agentName": "a",
			"status": "RUN_STATUS_SUCCEEDED", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
			"labels": map[string]string{
				"chat.conversation": "conv_bob", "chat.user": "bob", "chat.app": appName,
			},
		},
	)
	// A fresh server: nothing in its store but the accounts.
	server := newTestServer(t, daemon)
	alice, bob := signIn(t, server, "alice"), signIn(t, server, "bob")

	// The sidebar is the local index, which knows nothing yet.
	listed := call(t, server, alice, http.MethodGet, "/api/conversations", "")
	var page struct {
		Conversations []map[string]any `json:"conversations"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Conversations) != 0 {
		t.Fatalf("the sidebar invented rows it has no record of: %v", page.Conversations)
	}

	// Ownership comes from the label, with no local record to consult.
	if recorder := call(t, server, bob, http.MethodGet, "/api/conversations/conv_alice", ""); recorder.Code != http.StatusNotFound {
		t.Errorf("a stranger reached a conversation the store never recorded: %d", recorder.Code)
	}
	if recorder := call(t, server, alice, http.MethodGet, "/api/conversations/conv_alice", ""); recorder.Code != http.StatusOK {
		t.Errorf("the owner could not open their own conversation: %d %s", recorder.Code, recorder.Body)
	}

	// Asked explicitly, the index is rebuilt from the daemon.
	rebuilt := call(t, server, alice, http.MethodPost, "/api/conversations/recover", "")
	if rebuilt.Code != http.StatusOK {
		t.Fatalf("recover: %d %s", rebuilt.Code, rebuilt.Body)
	}
	listed = call(t, server, alice, http.MethodGet, "/api/conversations", "")
	if err := json.Unmarshal(listed.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Conversations) != 1 || page.Conversations[0]["id"] != "conv_alice" {
		t.Fatalf("recover did not rebuild the list: %v", page.Conversations)
	}
	if bobs := call(t, server, bob, http.MethodPost, "/api/conversations/recover", ""); !strings.Contains(bobs.Body.String(), `"recovered":1`) {
		t.Errorf("bob recovered %s, want only his own conversation", bobs.Body)
	}
}

// A conversation nobody has written to yet has no run behind it, so the daemon
// has never heard of it. It still belongs in its owner's list.
func TestAConversationThatHasNotRunYetIsStillListed(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	alice := signIn(t, server, "alice")
	id := createConversation(t, server, alice)

	listed := call(t, server, alice, http.MethodGet, "/api/conversations", "")
	var page struct {
		Conversations []map[string]any `json:"conversations"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Conversations) != 1 || page.Conversations[0]["id"] != id {
		t.Fatalf("a conversation with no run yet vanished from the list: %v", page.Conversations)
	}
	if page.Conversations[0]["started"] != false {
		t.Errorf("it should not claim to have run: %v", page.Conversations[0])
	}
}

// An idle conversation must give its environment back. The daemon keeps a
// detached run alive when the browser goes away, so closing the stream left
// one run — and the sandbox it holds — per conversation ever opened.
func TestReleasingAnIdleConversationEndsItsRun(t *testing.T) {
	daemon := fakeDaemon(t, nil)
	stopped := make(chan string, 4)
	daemon.onStopRun = func(runID string) { stopped <- runID }
	daemon.listRuns(map[string]any{
		// The same id the stub's attach reports, so the run this conversation
		// holds and the one the daemon lists are the same run.
		"runId": "run_1", "projectId": "p", "agentName": "a",
		"status": "RUN_STATUS_RUNNING", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		"labels": map[string]string{
			"chat.conversation": "conv_idle", "chat.user": "alice", "chat.app": appName,
		},
	})
	server := newTestServer(t, daemon)
	record := conversationRecord{ID: "conv_idle", Owner: "alice", ProjectID: "p", AgentName: "a", Started: true}
	if _, err := server.store.remember(record); err != nil {
		t.Fatalf("remember: %v", err)
	}
	found, err := server.attach(context.Background(), record)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	// Nobody is watching and nothing is running: exactly what the sweep looks for.
	if !found.releasable(0) {
		t.Fatal("a conversation with no viewers should be releasable")
	}
	go server.release(context.Background(), 10*time.Millisecond, 0)

	select {
	case runID := <-stopped:
		if runID != "run_1" {
			t.Fatalf("released the wrong run: %s", runID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an idle conversation kept its run, and so kept its sandbox, forever")
	}
}

// Shutting down owes the daemon the same thing the idle sweep does. A process
// that exits without ending its sessions leaves a detached run — and the
// sandbox it holds — behind for every conversation it had open, and nothing
// on the daemon side expires them.
func TestShuttingDownEndsEverySessionItHolds(t *testing.T) {
	daemon := fakeDaemon(t, nil)
	stopped := make(chan string, 8)
	daemon.onStopRun = func(runID string) { stopped <- runID }
	daemon.listRuns(map[string]any{
		"runId": "run_1", "projectId": "p", "agentName": "a",
		"status": "RUN_STATUS_RUNNING", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		"labels": map[string]string{
			"chat.conversation": "conv_open", "chat.user": "alice", "chat.app": appName,
		},
	})
	server := newTestServer(t, daemon)
	record := conversationRecord{ID: "conv_open", Owner: "alice", ProjectID: "p", AgentName: "a", Started: true}
	if _, err := server.store.remember(record); err != nil {
		t.Fatalf("remember: %v", err)
	}
	found, err := server.attach(context.Background(), record)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	// Somebody is watching, so the idle sweep would never touch this one.
	found.join()

	server.closeAll(context.Background())

	select {
	case runID := <-stopped:
		if runID != "run_1" {
			t.Fatalf("shutdown ended the wrong run: %s", runID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown left a watched conversation's run running")
	}
	if len(server.sessions) != 0 {
		t.Errorf("shutdown left %d sessions behind", len(server.sessions))
	}
}

// Two requests opening the same conversation at once must resume its run once.
// Both would otherwise call Open, and only one of them could hold the run's
// input lease; the other had to give the run up and start a new one. Worse,
// whichever handle lost the race to publish itself was closed — and closing a
// handle that did attach stops its run, killing the turn the survivor needed.
func TestOpeningOneConversationTwiceAtOnceResumesItOnce(t *testing.T) {
	daemon := quietDaemon(t)
	stopped := make(chan string, 4)
	daemon.onStopRun = func(runID string) { stopped <- runID }
	daemon.listRuns(map[string]any{
		"runId": "run_1", "projectId": "p", "agentName": "a",
		"status": "RUN_STATUS_RUNNING", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		"labels": map[string]string{
			"chat.conversation": "conv_race", "chat.user": "alice", "chat.app": appName,
		},
	})
	// Holding the run lookup keeps the first attach inside the daemon call
	// long enough for the others to pile up behind it, which is the window
	// this is about.
	release := daemon.hold("conv_race")

	server := newTestServer(t, daemon)
	record := conversationRecord{ID: "conv_race", Owner: "alice", ProjectID: "p", AgentName: "a", Started: true}
	if _, err := server.store.remember(record); err != nil {
		t.Fatalf("remember: %v", err)
	}

	const racers = 4
	ready := make(chan struct{}, racers)
	opened := make(chan *session, racers)
	failed := make(chan error, racers)
	for i := 0; i < racers; i++ {
		go func() {
			ready <- struct{}{}
			found, err := server.attach(context.Background(), record)
			if err != nil {
				failed <- err
				return
			}
			opened <- found
		}()
	}
	for i := 0; i < racers; i++ {
		<-ready
	}
	release()

	var first *session
	for i := 0; i < racers; i++ {
		select {
		case err := <-failed:
			t.Fatalf("attach: %v", err)
		case found := <-opened:
			if first == nil {
				first = found
			} else if found != first {
				t.Fatal("concurrent attaches produced two sessions for one conversation")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("attach never returned")
		}
	}
	// A start frame is written before the daemon has read it, so a second
	// resume would land shortly after its racer returned, not before.
	deadline := time.Now().Add(10 * time.Second)
	for daemon.attachCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if attaches := daemon.attachCount(); attaches != 1 {
		t.Fatalf("resumed the conversation %d times, want 1", attaches)
	}
	select {
	case runID := <-stopped:
		t.Fatalf("a losing handle stopped run %s, which the surviving one is holding", runID)
	default:
	}
}
