package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"github.com/chaitin/agent-compose/sdk/go/chat"
)

// ownerLabel and appLabel are attached to every run this server starts, so a
// conversation is attributable on the daemon side without consulting this
// server's own state file.
// releaseTimeout bounds the daemon calls that end one idle conversation's run.
const releaseTimeout = 30 * time.Second

const (
	ownerLabel = "chat.user"
	appLabel   = "chat.app"
	// appName is written onto every run this server starts and is also what
	// the conversation lookups filter on, so it identifies data already on the
	// daemon rather than this program. Renaming it would hide every
	// conversation started before the rename.
	appName = "chatui"
)

type uiServer struct {
	client   *chat.Client
	daemon   string
	token    string
	store    *store
	auth     *authenticator
	upgrader websocket.Upgrader

	mu       sync.Mutex
	sessions map[string]*session
	// opening gates the conversations being opened right now, so a second
	// request waits for the first instead of racing it to the same run.
	opening map[string]chan struct{}
}

// routes wires every endpoint. Everything but signing in, the page itself and
// the font it is set in requires a session.
func (s *uiServer) routes() *http.ServeMux {
	mux := http.NewServeMux()
	guard := s.auth.guard

	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /fonts/{name}", s.font)
	mux.HandleFunc("POST /api/login", s.auth.signIn)
	mux.HandleFunc("POST /api/logout", s.auth.signOut)
	mux.HandleFunc("GET /api/me", s.auth.whoami)

	mux.HandleFunc("GET /api/agents", guard(s.agents))
	mux.HandleFunc("GET /api/conversations", guard(s.list))
	mux.HandleFunc("POST /api/conversations/recover", guard(s.reindex))
	mux.HandleFunc("POST /api/conversations", guard(s.create))
	mux.HandleFunc("GET /api/conversations/{id}", guard(s.describe))
	mux.HandleFunc("PATCH /api/conversations/{id}", guard(s.rename))
	mux.HandleFunc("DELETE /api/conversations/{id}", guard(s.remove))
	mux.HandleFunc("GET /api/socket", guard(s.socket))
	return mux
}

// release ends the runs of conversations nobody is watching, so an unattended
// conversation costs neither this process nor the daemon anything.
//
// Closing the stream alone was not enough. A conversation attaches with the
// detach disconnect policy — a turn must survive the browser going away — so
// the daemon keeps the run alive when the stream drops, and the run is what
// holds the environment. Every conversation ever opened therefore kept one
// running until somebody retired it by hand.
//
// Ending the run is not ending the conversation: durable history and identity
// are untouched, and the next message starts a run that resumes this same
// sandbox with the agent's context intact.
//
// release runs until ctx is done, which is what lets shutdown stop it before
// it closes the same sessions closeAll is closing.
func (s *uiServer) release(ctx context.Context, every, after time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, found := range s.takeIdle(after) {
			s.endSession(ctx, found, "idle")
		}
	}
}

// takeIdle removes the sessions that have gone quiet from the live set and
// returns them, so the daemon calls that end them happen off the lock.
func (s *uiServer) takeIdle(after time.Duration) []*session {
	s.mu.Lock()
	defer s.mu.Unlock()
	idle := make([]*session, 0, len(s.sessions))
	for id, found := range s.sessions {
		if found.releasable(after) {
			idle = append(idle, found)
			delete(s.sessions, id)
		}
	}
	return idle
}

// closeAll ends every live session, which is what shutdown owes the daemon:
// a killed process leaves its runs detached, and nothing on the daemon side
// expires them.
func (s *uiServer) closeAll(ctx context.Context) {
	s.mu.Lock()
	live := make([]*session, 0, len(s.sessions))
	for id, found := range s.sessions {
		live = append(live, found)
		delete(s.sessions, id)
	}
	s.mu.Unlock()
	for _, found := range live {
		s.endSession(ctx, found, "shutting down")
	}
}

// endSession closes one conversation, bounded so a daemon that has stopped
// answering cannot hold the sweep — or the shutdown — open.
func (s *uiServer) endSession(ctx context.Context, found *session, why string) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	// Read before Close, which clears it: the run is the thing that failed to
	// be released, so naming it is the point of the warning.
	runID := found.runID()
	if err := found.conversation.Close(closeCtx); err != nil {
		slog.Warn("conversation not released", "conversation", found.record.ID, "run", runID, "reason", why, "error", err)
		return
	}
	slog.Debug("conversation released", "conversation", found.record.ID, "run", runID, "reason", why)
}

func (s *uiServer) index(w http.ResponseWriter, _ *http.Request) {
	page, err := assets.ReadFile("public/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is the whole app and is embedded in the binary, so a rebuilt
	// server must not be shadowed by a copy the browser decided to keep.
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(page)
}

// font serves the pixel typeface the page is set in. It ships inside the
// binary like the page does, so the UI looks the same offline and on a network
// that cannot reach a font CDN. Only font files are served: the licences
// embedded beside them travel with the binary, not to the browser.
func (s *uiServer) font(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !strings.HasSuffix(name, ".woff2") {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "font/woff2")
	// The file name carries the font's release, so a new font is a new URL and
	// this one can be kept for good. Unlike the page, there is no rebuilt copy
	// for a cached one to shadow.
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	http.ServeFileFS(w, r, assets, "public/fonts/"+name)
}

// list draws the sidebar from this server's own index.
//
// The daemon could answer it — every run carries chat.user and
// chat.conversation — but only at one GetRun per run: a run summary carries no
// labels by design, so grouping a page of runs into conversations means
// reading each one's detail. The sidebar is redrawn after every turn, so it
// reads the local index instead and rebuilds it on demand (see reindex).
func (s *uiServer) list(w http.ResponseWriter, r *http.Request) {
	records := s.store.conversations(userOf(r))
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		rows = append(rows, s.describeRecord(record))
	}
	writeJSON(w, map[string]any{"conversations": rows})
}

// reindex rebuilds the index from the daemon, for when this server has lost it
// or never had it: a new machine, a deleted state file, a second instance.
//
// This is the expensive path the sidebar deliberately avoids, so it runs only
// when asked. Conversations already indexed keep their titles; recovered ones
// have none, because a title was never the daemon's to hold.
func (s *uiServer) reindex(w http.ResponseWriter, r *http.Request) {
	owner := userOf(r)
	known, err := s.client.Conversations(r.Context(), chat.Search{
		Labels: map[string]string{ownerLabel: owner, appLabel: appName},
	})
	if err != nil {
		writeError(w, err)
		return
	}
	added := 0
	for _, found := range known {
		if _, err := s.store.conversation(owner, found.ID); err == nil {
			continue
		}
		if _, err := s.store.remember(recordOf(owner, found)); err != nil {
			writeError(w, err)
			return
		}
		added++
	}
	writeJSON(w, map[string]any{"recovered": added, "found": len(known)})
}

// recordOf indexes a conversation only the daemon knows about.
func recordOf(owner string, found chat.ConversationInfo) conversationRecord {
	return conversationRecord{
		ID:        found.ID,
		Owner:     owner,
		ProjectID: found.ProjectID,
		AgentName: found.AgentName,
		Started:   true,
		CreatedAt: found.LastActive,
		UpdatedAt: found.LastActive,
	}
}

// create starts a new conversation. No daemon call is made: the environment is
// provisioned by the first message, so an abandoned "new chat" costs nothing.
func (s *uiServer) create(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ProjectID string `json:"projectId"`
		AgentName string `json:"agentName"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		http.Error(w, "malformed request", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(body.ProjectID) == "" || strings.TrimSpace(body.AgentName) == "" {
		http.Error(w, "projectId and agentName are required", http.StatusBadRequest)
		return
	}
	owner := userOf(r)
	// The labels go on at creation, not at attach: they are what makes the run
	// this conversation eventually starts attributable to its owner, and the
	// start frame is the only chance to set them.
	conversation := s.client.Agent(body.ProjectID, body.AgentName).Start(s.labels(owner))
	record := conversationRecord{
		ID:        conversation.ID(),
		Owner:     owner,
		ProjectID: body.ProjectID,
		AgentName: body.AgentName,
	}
	record, err := s.store.remember(record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	s.sessions[record.ID] = newSession(conversation, record)
	s.mu.Unlock()
	writeJSON(w, s.describeRecord(record))
}

// describe returns one conversation with its transcript, which is what opening
// it from the sidebar needs. Reading history does not attach: that happens when
// the socket opens.
func (s *uiServer) describe(w http.ResponseWriter, r *http.Request) {
	record, ok := s.record(w, r)
	if !ok {
		return
	}
	payload := s.describeRecord(record)
	messages := []map[string]any{}
	if record.Started {
		history, err := readConversationHistory(r.Context(), s.handle(record))
		if err != nil {
			writeError(w, err)
			return
		}
		for _, message := range history {
			messages = append(messages, map[string]any{
				"role": message.Role, "text": message.Text, "time": message.Time,
			})
		}
	}
	payload["messages"] = messages
	writeJSON(w, payload)
}

// readConversationHistory walks the SDK's paged history API from newest to
// oldest, then restores chronological order for the browser transcript.
func readConversationHistory(ctx context.Context, conversation *chat.Conversation) ([]chat.Message, error) {
	var pages [][]chat.Message
	cursor := ""
	for {
		page, err := conversation.HistoryPage(ctx, chat.HistoryOptions{Limit: 200, Before: cursor})
		if err != nil {
			return nil, err
		}
		if len(page.Messages) > 0 {
			pages = append(pages, page.Messages)
		}
		if page.Cursor == "" {
			break
		}
		cursor = page.Cursor
	}
	total := 0
	for _, page := range pages {
		total += len(page)
	}
	history := make([]chat.Message, 0, total)
	for i := len(pages) - 1; i >= 0; i-- {
		history = append(history, pages[i]...)
	}
	return history, nil
}

func (s *uiServer) rename(w http.ResponseWriter, r *http.Request) {
	record, ok := s.record(w, r)
	if !ok {
		return
	}
	var body struct {
		Title string `json:"title"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10)).Decode(&body); err != nil {
		http.Error(w, "malformed request", http.StatusBadRequest)
		return
	}
	if err := s.store.rename(record.Owner, record.ID, body.Title); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	updated, err := s.store.conversation(record.Owner, record.ID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, s.describeRecord(updated))
}

// remove archives the conversation on the daemon and drops it from the
// sidebar. Archiving stops the sandbox; the daemon keeps the conversation's
// durable history and identity, this server just stops listing it.
//
// It works whether or not this process is currently attached: a conversation
// that was released while nobody watched still has to be removable.
func (s *uiServer) remove(w http.ResponseWriter, r *http.Request) {
	record, ok := s.record(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	live, attached := s.sessions[record.ID]
	delete(s.sessions, record.ID)
	s.mu.Unlock()

	var failure error
	switch {
	case attached:
		// This process holds the session, so closing it ends the run and the
		// daemon stops the environment with it.
		failure = live.conversation.Close(r.Context())
	case record.Started:
		// No handle here — after a restart there is never one — but the
		// conversation may still have a run the daemon kept alive. It is
		// reachable by ID through the label its runs carry.
		failure = s.client.EndSession(r.Context(), record.ID)
	}
	if err := s.store.forget(record.Owner, record.ID); err != nil && !errors.Is(err, errNoSuchConversation) {
		writeError(w, err)
		return
	}
	// The sidebar entry is gone either way. A daemon that could not be reached
	// is reported, not hidden, but it does not resurrect the row.
	if failure != nil {
		writeJSON(w, map[string]any{"deleted": true, "warning": failure.Error()})
		return
	}
	writeJSON(w, map[string]any{"deleted": true})
}

// attach returns the live session for a conversation, opening one if this
// process is not currently holding it.
//
// One conversation is opened once at a time. Two requests opening the same
// conversation concurrently would both resume its run, and only one of them
// could hold that run's input lease; the other would have to give the run up
// and start a new one. Worse, whichever handle lost the race to publish
// itself was then closed — and closing a handle that did attach stops its
// run, which is not always the loser's. Making the second request wait for
// the first removes both, along with one source of lease conflicts.
func (s *uiServer) attach(ctx context.Context, record conversationRecord) (*session, error) {
	for {
		s.mu.Lock()
		if found, ok := s.sessions[record.ID]; ok {
			s.mu.Unlock()
			return found, nil
		}
		if opening, ok := s.opening[record.ID]; ok {
			s.mu.Unlock()
			select {
			case <-opening:
				// The opener has published its session, or failed and left the
				// conversation unheld. Look again rather than assuming which.
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if s.opening == nil {
			s.opening = map[string]chan struct{}{}
		}
		opening := make(chan struct{})
		s.opening[record.ID] = opening
		s.mu.Unlock()

		opened, err := s.open(ctx, record)

		s.mu.Lock()
		delete(s.opening, record.ID)
		if err == nil {
			s.sessions[record.ID] = opened
		}
		s.mu.Unlock()
		close(opening)
		if err != nil {
			return nil, err
		}
		return opened, nil
	}
}

// open builds the session behind one record, which is the part that talks to
// the daemon. Callers hold the conversation's opening gate.
//
// A conversation that has never run is started rather than opened: opening one
// with no runs behind it would report it as restarted, which would be a lie
// about a conversation that has nothing to restart.
func (s *uiServer) open(ctx context.Context, record conversationRecord) (*session, error) {
	agent := s.client.Agent(record.ProjectID, record.AgentName)
	if !record.Started {
		return newSession(agent.Start(chat.WithID(record.ID), s.labels(record.Owner)), record), nil
	}
	resumed, err := agent.Open(ctx, record.ID, s.labels(record.Owner))
	if err != nil {
		return nil, err
	}
	return newSession(resumed, record), nil
}

// handle returns a conversation handle for the operations that need no live
// stream: reading history, and archiving.
func (s *uiServer) handle(record conversationRecord) *chat.Conversation {
	agent := s.client.Agent(record.ProjectID, record.AgentName)
	return agent.Start(chat.WithID(record.ID), s.labels(record.Owner))
}

// labels mark the run on the daemon with who it belongs to, so a conversation
// can be traced back to a person from the daemon's own tooling.
func (s *uiServer) labels(owner string) chat.Option {
	return chat.WithLabels(map[string]string{ownerLabel: owner, appLabel: appName})
}

// describeRecord renders one sidebar row.
func (s *uiServer) describeRecord(record conversationRecord) map[string]any {
	s.mu.Lock()
	live, attached := s.sessions[record.ID]
	s.mu.Unlock()
	row := map[string]any{
		"id":        record.ID,
		"projectId": record.ProjectID,
		"agentName": record.AgentName,
		"title":     record.Title,
		"started":   record.Started,
		"createdAt": record.CreatedAt,
		"updatedAt": record.UpdatedAt,
		"attached":  attached,
		"running":   attached && live.running(),
	}
	if attached {
		row["continuity"] = live.conversation.Continuity()
	}
	return row
}

// record resolves the path's conversation, and only for the user who owns it.
//
// The local index is consulted first because it is free, but a miss is not an
// answer: ownership lives in the chat.user label on the daemon, not here.
// Lookup settles it in one call — the run list's label filter answers "is
// there a conversation with this ID carrying these labels?" without reading a
// single label back — so a conversation still works after this server has lost
// or never had a record of it.
func (s *uiServer) record(w http.ResponseWriter, r *http.Request) (conversationRecord, bool) {
	found, err := s.findRecord(r.Context(), userOf(r), r.PathValue("id"))
	if err != nil {
		if !errors.Is(err, errNoSuchConversation) {
			writeError(w, err)
			return conversationRecord{}, false
		}
		// Someone else's conversation is reported absent rather than
		// forbidden: that it exists is none of the asker's business.
		http.Error(w, "unknown conversation", http.StatusNotFound)
		return conversationRecord{}, false
	}
	return found, true
}

// findRecord resolves one conversation for one signed-in user. A local record
// is only an index: started conversations are checked against the daemon's
// ownership labels before they are used for history, attach, or control.
func (s *uiServer) findRecord(ctx context.Context, owner, id string) (conversationRecord, error) {
	local, localErr := s.store.conversation(owner, id)
	if localErr == nil && !local.Started {
		return local, nil
	}
	if localErr != nil && !errors.Is(localErr, errNoSuchConversation) {
		return conversationRecord{}, localErr
	}
	found, ok, err := s.client.Lookup(ctx, id, map[string]string{ownerLabel: owner, appLabel: appName})
	if err != nil {
		return conversationRecord{}, err
	}
	if !ok {
		return conversationRecord{}, errNoSuchConversation
	}
	if localErr == nil {
		// Preserve the local title and timestamps while taking the daemon's
		// project/agent identity as the authoritative attachment target.
		local.ProjectID, local.AgentName, local.Started = found.ProjectID, found.AgentName, true
		return local, nil
	}
	return recordOf(owner, found), nil
}

func writeJSON(w http.ResponseWriter, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, chat.ErrNotFound), errors.Is(err, errNoSuchConversation):
		status = http.StatusNotFound
	case errors.Is(err, chat.ErrInvalidArgument):
		status = http.StatusBadRequest
	case errors.Is(err, chat.ErrPermission):
		status = http.StatusForbidden
	}
	http.Error(w, err.Error(), status)
}

// truncate shortens text to limit bytes without splitting a rune, so a cut
// Chinese title stays readable instead of ending in a replacement character.
func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…"
}
