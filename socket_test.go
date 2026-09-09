package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"connectrpc.com/connect"
	"github.com/chaitin/agent-compose/sdk/go/chat"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentcomposev2 "github.com/chaitin/agent-compose/proto/agentcompose/v2"
	"github.com/chaitin/agent-compose/proto/agentcompose/v2/agentcomposev2connect"
)

// daemonStub stands in for agent-compose.
//
// It serves unencrypted HTTP/2 because a Connect bidirectional stream needs
// HTTP/2, and it writes no response header until it has read the client's
// opening frame, which is how the real daemon behaves.
type daemonStub struct {
	*httptest.Server

	mu            sync.Mutex
	runs          []map[string]any
	start         *agentcomposev2.AttachAgentRunStart
	projects      []map[string]any
	projectAgents map[string][]map[string]any
	held          map[string]chan struct{}
	// attaches counts the AttachAgentRun streams opened, which is how a test
	// sees a conversation being resumed more than once.
	attaches int
	// onStopRun observes the run a caller asked the daemon to end.
	onStopRun func(runID string)
	// failTurn, when set, ends the attach with a failed result instead of a
	// completed turn, which is how a provider error reaches the browser.
	failTurn string
}

// hold makes the run lookup for one conversation block until the returned
// function is called. It is how a test puts one conversation behind a slow
// daemon and watches whether the others on the same socket carry on.
func (d *daemonStub) hold(conversation string) func() {
	gate := make(chan struct{})
	d.mu.Lock()
	if d.held == nil {
		d.held = map[string]chan struct{}{}
	}
	d.held[conversation] = gate
	d.mu.Unlock()
	return sync.OnceFunc(func() {
		d.mu.Lock()
		delete(d.held, conversation)
		d.mu.Unlock()
		close(gate)
	})
}

func (d *daemonStub) gate(conversation string) chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.held[conversation]
}

// attachCount reports how many attach streams the daemon has been asked for.
func (d *daemonStub) attachCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attaches
}

// startFrame returns the request the last attach opened with, so a test can
// check what the run was actually created with.
func (d *daemonStub) startFrame() *agentcomposev2.AttachAgentRunStart {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.start
}

// listRuns sets what ListRuns reports, which is where the chat list comes from.
func (d *daemonStub) listRuns(runs ...map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runs = runs
}

func (d *daemonStub) currentRuns() []map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.runs)
}

// setProjectCatalog sets the project and agent records returned by the two
// ProjectService calls used by the UI's new-conversation selector.
func (d *daemonStub) setProjectCatalog(projects []map[string]any, agents map[string][]map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.projects = projects
	d.projectAgents = agents
}

func (d *daemonStub) projectCatalog() ([]map[string]any, map[string][]map[string]any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.projects), d.projectAgents
}

func fakeDaemon(t *testing.T, script []wireEvent) *daemonStub {
	t.Helper()
	stub := &daemonStub{}
	mux := http.NewServeMux()
	// The RunService is served by the generated Connect handler: the codec,
	// framing and procedure paths under test are then the real ones, and a
	// client that speaks binary protobuf is answered in kind. The remaining
	// services stay hand-written JSON because the UI server addresses them
	// that way itself, without going through the chat SDK.
	mux.Handle(agentcomposev2connect.NewRunServiceHandler(&runService{stub: stub, script: script, t: t}))
	mux.HandleFunc("POST /agentcompose.v2.SandboxService/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{})
	})
	mux.Handle(agentcomposev2connect.NewProjectServiceHandler(&projectService{stub: stub}))
	stub.Server = httptest.NewUnstartedServer(mux)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	stub.Config.Protocols = protocols
	stub.Start()
	t.Cleanup(stub.Close)
	return stub
}

type wireEvent struct {
	name    string
	payload map[string]any
}

// readEnvelope consumes one Connect stream frame.
func readEnvelope(body io.Reader) ([]byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(body, header); err != nil {
		return nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header[1:]))
	if _, err := io.ReadFull(body, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func subscribeSocket(t *testing.T, conn *websocket.Conn, id string) {
	t.Helper()
	if err := conn.WriteJSON(map[string]string{"type": "subscribe", "conversationId": id}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var frame map[string]any
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read subscribed: %v", err)
	}
	if frame["type"] != "subscribed" || frame["conversationId"] != id {
		t.Fatalf("subscribe response = %v", frame)
	}
}

// A turn's events reach the browser with the step each one names, and a tool
// result stays joined to the call it answers. Both are what the UI needs to
// draw the work as steps rather than as a flat log.
func TestTheSocketRelaysATurnAsStructuredSteps(t *testing.T) {
	daemon := fakeDaemon(t, []wireEvent{
		{"step_start", map[string]any{"step": 0}},
		{"reasoning_delta", map[string]any{"step": 0, "text": "先看看目录"}},
		{"tool_call", map[string]any{
			"step": 0, "id": "call_1", "name": "bash",
			"toolKind": "execute", "status": "in_progress", "command": "ls /",
		}},
		{"tool_result", map[string]any{"step": 0, "id": "call_1", "ok": true, "output": "bin\netc\n"}},
		{"step_end", map[string]any{"step": 0, "stopReason": "tool_use"}},
		{"step_start", map[string]any{"step": 1}},
		{"text_delta", map[string]any{"step": 1, "text": "根目录里有 "}},
		{"text_delta", map[string]any{"step": 1, "text": "bin 和 etc。"}},
		{"usage", map[string]any{"step": 1, "scope": "turn", "inputTokens": 120, "outputTokens": 30}},
		{"step_end", map[string]any{"step": 1, "stopReason": "stop"}},
		{"step_end", map[string]any{"scope": "run", "stopReason": "stop"}},
	})

	server := newTestServer(t, daemon)

	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)

	browser := newBrowser(t, front.URL)
	id := browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	dialer := &websocket.Dialer{Jar: browser.Jar}
	conn, handshake, err := dialer.Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()

	subscribeSocket(t, conn, id)
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": id, "text": "根目录有什么"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	var kinds []string
	var steps []any
	tools := map[string]map[string]any{}
	answer := ""
	var runEnd map[string]any
	var done map[string]any
	for done == nil {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read: %v (collected %v)", err, kinds)
		}
		switch frame["type"] {
		case "turn_started":
			if frame["prompt"] != "根目录有什么" {
				t.Errorf("turn_started did not carry the prompt: %v", frame)
			}
		case "event":
			event, _ := frame["event"].(map[string]any)
			kind, _ := event["kind"].(string)
			kinds = append(kinds, kind)
			if kind == "text_delta" {
				answer += event["text"].(string)
			}
			if kind == "tool_call" || kind == "tool_result" {
				tools[kind] = event
			}
			if kind == "step_end" && event["scope"] == "run" {
				runEnd = event
			}
			if step, present := event["step"]; present {
				steps = append(steps, step)
			}
		case "error":
			t.Fatalf("the turn failed: %v", frame)
		case "turn_done":
			done = frame
		}
	}

	want := []string{
		"step_start", "reasoning_delta", "tool_call", "tool_result", "step_end",
		"step_start", "text_delta", "text_delta", "usage", "step_end", "step_end",
	}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Errorf("relayed %v, want %v", kinds, want)
	}
	if len(steps) != len(want)-1 {
		t.Errorf("%d events carried a step, want every event except the run terminator", len(steps))
	}
	if runEnd == nil {
		t.Error("the run-level step end lost its scope")
	}
	if tools["tool_call"]["id"] != tools["tool_result"]["id"] {
		t.Errorf("a tool result lost the call it answers: %v / %v", tools["tool_call"], tools["tool_result"])
	}
	if tools["tool_call"]["command"] != "ls /" {
		t.Errorf("the tool call lost its command: %v", tools["tool_call"])
	}
	if answer != "根目录里有 bin 和 etc。" {
		t.Errorf("the answer did not accumulate: %q", answer)
	}
	if done["continuity"] != string(chat.Continuous) {
		t.Errorf("turn_done reported continuity %v", done["continuity"])
	}
}

// A turn also names its conversation, so the sidebar row appears without the
// page having to guess a title or poll for one.
func TestTheFirstMessageNamesTheConversationOverTheSocket(t *testing.T) {
	daemon := fakeDaemon(t, []wireEvent{{"text_delta", map[string]any{"text": "好的"}}})

	server := newTestServer(t, daemon)

	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	id := browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()
	subscribeSocket(t, conn, id)
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": id, "text": "帮我看下日志\n第二行"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read: %v", err)
		}
		if frame["type"] != "conversation" {
			continue
		}
		row, _ := frame["conversation"].(map[string]any)
		if row["title"] != "帮我看下日志" {
			t.Fatalf("the sidebar row was not named after the first message: %v", row)
		}
		if row["started"] != true {
			t.Fatalf("the row does not record that the conversation has run: %v", row)
		}
		return
	}
}

// newBrowser signs in and keeps the cookie, the way a page does.
func newBrowser(t *testing.T, base string) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("jar: %v", err)
	}
	client := &http.Client{Jar: jar, Timeout: 15 * time.Second}
	response, err := client.Post(base+"/api/login", "application/json",
		strings.NewReader(`{"user":"alice","password":"alice-password"}`))
	if err != nil {
		t.Fatalf("sign in: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("sign in: %s", response.Status)
	}
	if parsed, err := url.Parse(base); err == nil && len(jar.Cookies(parsed)) == 0 {
		t.Fatal("sign in left no cookie")
	}
	return client
}

func browserCreate(t *testing.T, browser *http.Client, base string) string {
	t.Helper()
	response, err := browser.Post(base+"/api/conversations", "application/json",
		strings.NewReader(`{"projectId":"p","agentName":"a"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return created.ID
}

func TestOneSocketMultiplexesConversations(t *testing.T) {
	daemon := fakeDaemon(t, []wireEvent{{"text_delta", map[string]any{"text": "完成"}}})
	server := newTestServer(t, daemon)
	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	first, second := browserCreate(t, browser, front.URL), browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()
	subscribeSocket(t, conn, first)
	subscribeSocket(t, conn, second)
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": first, "text": "第一个"}); err != nil {
		t.Fatalf("send first: %v", err)
	}
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": second, "text": "第二个"}); err != nil {
		t.Fatalf("send second: %v", err)
	}

	// Both the turn and the sidebar row are waited for. They are produced by
	// different goroutines, so leaving before the row is written would let the
	// server still be writing its state file after the test tore its directory
	// down.
	done, named := map[string]bool{}, map[string]bool{}
	for len(done) < 2 || len(named) < 2 {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read: %v", err)
		}
		id, _ := frame["conversationId"].(string)
		switch frame["type"] {
		case "turn_done":
			done[id] = true
		case "conversation":
			named[id] = true
		}
	}
	if !done[first] || !done[second] {
		t.Fatalf("turns were not routed for both conversations: %v", done)
	}
}

// One conversation waiting on the daemon must not hold up the others sharing
// the socket. The frames of every conversation arrive on one connection, so a
// server that handled them on the read loop would turn any slow conversation
// into a freeze of every other one — which is the thing multiplexing them was
// supposed to avoid.
func TestASlowConversationDoesNotStallTheOthersOnOneSocket(t *testing.T) {
	daemon := quietDaemon(t)
	// A conversation this server has no record of: resolving it costs a daemon
	// lookup, which is the call the test holds open.
	daemon.listRuns(map[string]any{
		"runId": "run_slow", "projectId": "p", "agentName": "a",
		"status": "RUN_STATUS_RUNNING", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		"labels": map[string]string{
			"chat.conversation": "conv_slow", "chat.user": "alice", "chat.app": appName,
		},
	})
	release := daemon.hold("conv_slow")
	defer release()

	server := newTestServer(t, daemon)
	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	quick := browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()

	if err := conn.WriteJSON(map[string]string{"type": "subscribe", "conversationId": "conv_slow"}); err != nil {
		t.Fatalf("subscribe slow: %v", err)
	}
	// The held conversation answers nothing, so this ack can only arrive if the
	// second conversation was handled independently of the first.
	subscribeSocket(t, conn, quick)

	release()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read: %v", err)
		}
		if frame["type"] == "subscribed" && frame["conversationId"] == "conv_slow" {
			return
		}
		if frame["type"] == "error" {
			t.Fatalf("the held conversation failed once released: %v", frame)
		}
	}
}

// Subscribing to a conversation this socket already holds is answered again
// rather than dropped. A page that reconnects, or re-opens a conversation it
// never released, cannot always know which it is; silence would leave it
// waiting for a confirmation that never comes.
func TestSubscribingTwiceIsAcknowledgedTwice(t *testing.T) {
	server := newTestServer(t, quietDaemon(t))
	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	id := browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()

	subscribeSocket(t, conn, id)
	subscribeSocket(t, conn, id)
}

// A viewer that arrives after a turn ended is not shown that turn again. The
// finished turn is still the conversation's most recent one, and it is already
// in the transcript the page loads before it subscribes, so replaying it would
// draw the same exchange twice.
func TestAFinishedTurnIsNotReplayedToAViewerThatComesBack(t *testing.T) {
	daemon := fakeDaemon(t, []wireEvent{{"text_delta", map[string]any{"text": "好了"}}})
	server := newTestServer(t, daemon)
	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	id := browserCreate(t, browser, front.URL)
	// Once the first message has run, resolving the conversation goes through
	// the daemon, so the run it started has to be visible there.
	daemon.listRuns(map[string]any{
		"runId": "run_1", "projectId": "p", "agentName": "a",
		"status": "RUN_STATUS_SUCCEEDED", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		"labels": map[string]string{
			"chat.conversation": id, "chat.user": "alice", "chat.app": appName,
		},
	})

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()

	subscribeSocket(t, conn, id)
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": id, "text": "你好"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForFrame(t, conn, "turn_done")
	if err := conn.WriteJSON(map[string]string{"type": "unsubscribe", "conversationId": id}); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	waitForFrame(t, conn, "unsubscribed")

	// Coming back to the conversation, the way opening it again from the
	// sidebar does.
	subscribeSocket(t, conn, id)
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			return // Nothing was replayed, which is the point.
		}
		if frame["type"] == "turn_started" || frame["type"] == "turn_done" {
			t.Fatalf("a finished turn was replayed to a returning viewer: %v", frame)
		}
	}
}

// waitForFrame reads until the named frame type arrives.
func waitForFrame(t *testing.T, conn *websocket.Conn, want string) map[string]any {
	t.Helper()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("waiting for %s: %v", want, err)
		}
		if frame["type"] == want {
			return frame
		}
	}
}

func TestUnsubscribedConversationCannotQueueFrames(t *testing.T) {
	socketCtx, socketCancel := context.WithCancel(context.Background())
	defer socketCancel()
	subscriptionCtx, subscriptionCancel := context.WithCancel(socketCtx)
	subscriptionCancel()

	connection := &socketConnection{
		ctx:      socketCtx,
		outbound: make(chan any, 1),
	}
	if connection.sendContext(subscriptionCtx, "conv-a", map[string]any{"type": "event"}) {
		t.Fatal("a canceled subscription queued a frame")
	}
	select {
	case frame := <-connection.outbound:
		t.Fatalf("canceled subscription queued %v", frame)
	default:
	}
}

func TestSocketSubscriptionChecksConversationOwnership(t *testing.T) {
	daemon := quietDaemon(t)
	server := newTestServer(t, daemon)
	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	alice, bob := signIn(t, server, "alice"), signIn(t, server, "bob")
	id := createConversation(t, server, alice)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	header := http.Header{"Cookie": []string{bob.String()}}
	conn, handshake, err := (&websocket.Dialer{}).Dial(socketURL, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()
	if err := conn.WriteJSON(map[string]string{"type": "subscribe", "conversationId": id}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var frame map[string]any
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read: %v", err)
	}
	if frame["type"] != "error" || frame["conversationId"] != id {
		t.Fatalf("cross-user subscribe response = %v", frame)
	}
}

// The run a conversation starts must carry the labels the chat list is built
// from. They can only be set on the start frame, so a conversation created
// without them would be invisible to its own owner's sidebar forever after.
func TestTheRunAConversationStartsIsLabelledWithItsOwner(t *testing.T) {
	daemon := fakeDaemon(t, []wireEvent{{"text_delta", map[string]any{"text": "好"}}})
	server := newTestServer(t, daemon)

	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	id := browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()
	subscribeSocket(t, conn, id)
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": id, "text": "在吗"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read: %v", err)
		}
		if frame["type"] == "turn_done" {
			break
		}
	}

	labels := daemon.startFrame().GetRequest().GetLabels()
	if labels["chat.user"] != "alice" {
		t.Errorf("the run was not attributed to its owner: %v", labels)
	}
	if labels["chat.app"] != appName {
		t.Errorf("the run does not say which app started it: %v", labels)
	}
	if labels["chat.conversation"] != id {
		t.Errorf("the run does not carry its conversation identity: %v", labels)
	}
}

// A turn that fails is reported once. The reply carries a single error, and
// both Events and Wait hand it back — so relaying each of them put the same
// message on screen twice, one right under the other.
func TestAFailedTurnIsReportedOnce(t *testing.T) {
	daemon := fakeDaemon(t, []wireEvent{{"text_delta", map[string]any{"text": "开始"}}})
	daemon.failTurn = "the model is overloaded"
	server := newTestServer(t, daemon)
	front := httptest.NewServer(server.routes())
	t.Cleanup(front.Close)
	browser := newBrowser(t, front.URL)
	id := browserCreate(t, browser, front.URL)

	socketURL := strings.Replace(front.URL, "http://", "ws://", 1) + "/api/socket"
	conn, handshake, err := (&websocket.Dialer{Jar: browser.Jar}).Dial(socketURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = handshake.Body.Close() }()
	defer func() { _ = conn.Close() }()
	subscribeSocket(t, conn, id)
	if err := conn.WriteJSON(map[string]string{"type": "message", "conversationId": id, "text": "你好"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	errors := []string{}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			break // The failed turn produces no turn_done; quiet means finished.
		}
		if frame["type"] == "error" {
			message, _ := frame["message"].(string)
			errors = append(errors, message)
		}
	}
	if len(errors) != 1 {
		t.Fatalf("a failed turn produced %d error frames, want 1: %v", len(errors), errors)
	}
	if !strings.Contains(errors[0], "the model is overloaded") {
		t.Errorf("error frame lost the provider message: %q", errors[0])
	}
}

// runService answers the RunService calls the chat SDK makes, backed by the
// stub's fixtures. It replaces a hand-written Connect handler, so what the
// tests exercise is the generated server against the generated client.
type runService struct {
	agentcomposev2connect.UnimplementedRunServiceHandler

	stub   *daemonStub
	script []wireEvent
	t      *testing.T
}

func (s *runService) AttachAgentRun(_ context.Context, stream *connect.BidiStream[agentcomposev2.AttachAgentRunRequest, agentcomposev2.AttachAgentRunResponse]) error {
	opening, err := stream.Receive()
	if err != nil {
		return nil
	}
	d := s.stub
	d.mu.Lock()
	d.attaches++
	if start := opening.GetStart(); start != nil {
		d.start = start
	}
	d.mu.Unlock()

	send := func(frame *agentcomposev2.AttachAgentRunResponse) bool { return stream.Send(frame) == nil }

	send(&agentcomposev2.AttachAgentRunResponse{
		Frame: &agentcomposev2.AttachAgentRunResponse_Started{
			Started: &agentcomposev2.AttachStarted{RunId: "run_1", SandboxId: "sb_1"},
		},
	})
	for _, event := range s.script {
		payload, err := json.Marshal(event.payload)
		if err != nil {
			s.t.Errorf("encode %s: %v", event.name, err)
			return nil
		}
		send(&agentcomposev2.AttachAgentRunResponse{
			Frame: &agentcomposev2.AttachAgentRunResponse_AgentEvent{
				AgentEvent: &agentcomposev2.AttachAgentEvent{Name: event.name, PayloadJson: string(payload)},
			},
		})
	}
	if failure := d.turnFailure(); failure != "" {
		send(&agentcomposev2.AttachAgentRunResponse{
			Frame: &agentcomposev2.AttachAgentRunResponse_Result{
				Result: &agentcomposev2.AttachResult{Success: false, Error: failure},
			},
		})
		return nil
	}
	send(&agentcomposev2.AttachAgentRunResponse{
		Frame: &agentcomposev2.AttachAgentRunResponse_AgentTurnCompleted{
			AgentTurnCompleted: &agentcomposev2.AttachAgentTurnCompleted{RunId: "run_1"},
		},
	})
	return nil
}

func (s *runService) ListRuns(_ context.Context, request *connect.Request[agentcomposev2.ListRunsRequest]) (*connect.Response[agentcomposev2.ListRunsResponse], error) {
	wanted := request.Msg.GetLabels()
	if gate := s.stub.gate(wanted["chat.conversation"]); gate != nil {
		<-gate
	}
	matching := make([]*agentcomposev2.RunSummary, 0, len(s.stub.currentRuns()))
	for _, run := range s.stub.currentRuns() {
		labels, _ := run["labels"].(map[string]string)
		keep := true
		for key, value := range wanted {
			if labels[key] != value {
				keep = false
				break
			}
		}
		if keep {
			// A run summary carries no labels, which is the whole reason
			// grouping runs into conversations needs GetRun.
			matching = append(matching, runSummaryOf(run))
		}
	}
	return connect.NewResponse(&agentcomposev2.ListRunsResponse{Runs: matching, Total: uint32(len(matching))}), nil
}

func (s *runService) GetRun(_ context.Context, request *connect.Request[agentcomposev2.GetRunRequest]) (*connect.Response[agentcomposev2.GetRunResponse], error) {
	detail := &agentcomposev2.RunDetail{}
	for _, run := range s.stub.currentRuns() {
		if run["runId"] == request.Msg.GetRunId() {
			detail.Labels, _ = run["labels"].(map[string]string)
		}
	}
	return connect.NewResponse(&agentcomposev2.GetRunResponse{Run: detail}), nil
}

// ListRunEvents reports an empty transcript: these tests drive conversations
// through the socket rather than replaying durable history, and the handler
// this replaced answered the same way by falling through to a catch-all.
func (s *runService) ListRunEvents(_ context.Context, _ *connect.Request[agentcomposev2.ListRunEventsRequest]) (*connect.Response[agentcomposev2.ListRunEventsResponse], error) {
	return connect.NewResponse(&agentcomposev2.ListRunEventsResponse{}), nil
}

func (s *runService) StopRun(_ context.Context, request *connect.Request[agentcomposev2.StopRunRequest]) (*connect.Response[agentcomposev2.StopRunResponse], error) {
	if s.stub.onStopRun != nil {
		s.stub.onStopRun(request.Msg.GetRunId())
	}
	return connect.NewResponse(&agentcomposev2.StopRunResponse{}), nil
}

// runSummaryOf converts one fixture into the summary the daemon would report,
// so a test still writes its runs as plain maps.
func runSummaryOf(run map[string]any) *agentcomposev2.RunSummary {
	summary := &agentcomposev2.RunSummary{}
	if value, ok := run["runId"].(string); ok {
		summary.RunId = value
	}
	if value, ok := run["projectId"].(string); ok {
		summary.ProjectId = value
	}
	if value, ok := run["agentName"].(string); ok {
		summary.AgentName = value
	}
	if value, ok := run["sandboxId"].(string); ok {
		summary.SandboxId = value
	}
	if value, ok := run["status"].(string); ok {
		summary.Status = agentcomposev2.RunStatus(agentcomposev2.RunStatus_value[value])
	}
	if value, ok := run["createdAt"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
			summary.CreatedAt = timestamppb.New(parsed)
		}
	}
	return summary
}

// turnFailure reports the scripted provider failure, if the test set one.
func (d *daemonStub) turnFailure() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.failTurn
}

// projectService answers the catalog the chat SDK reads before a conversation
// can be started.
type projectService struct {
	agentcomposev2connect.UnimplementedProjectServiceHandler

	stub *daemonStub
}

func (p *projectService) ListProjects(_ context.Context, _ *connect.Request[agentcomposev2.ListProjectsRequest]) (*connect.Response[agentcomposev2.ListProjectsResponse], error) {
	fixtures, _ := p.stub.projectCatalog()
	projects := make([]*agentcomposev2.ProjectSummary, 0, len(fixtures))
	for _, project := range fixtures {
		summary := &agentcomposev2.ProjectSummary{}
		if value, ok := project["projectId"].(string); ok {
			summary.ProjectId = value
		}
		if value, ok := project["name"].(string); ok {
			summary.Name = value
		}
		projects = append(projects, summary)
	}
	return connect.NewResponse(&agentcomposev2.ListProjectsResponse{
		Projects: projects, Total: uint32(len(projects)),
	}), nil
}

func (p *projectService) GetProject(_ context.Context, request *connect.Request[agentcomposev2.GetProjectRequest]) (*connect.Response[agentcomposev2.GetProjectResponse], error) {
	id := request.Msg.GetProject().GetProjectId()
	_, byProject := p.stub.projectCatalog()
	agents := make([]*agentcomposev2.ProjectAgent, 0, len(byProject[id]))
	for _, agent := range byProject[id] {
		entry := &agentcomposev2.ProjectAgent{
			Enabled:      true,
			Availability: agentcomposev2.ProjectAgentAvailability_PROJECT_AGENT_AVAILABILITY_AVAILABLE,
		}
		if value, ok := agent["agentName"].(string); ok {
			entry.AgentName = value
		}
		if value, ok := agent["displayName"].(string); ok {
			entry.DisplayName = value
		}
		if value, ok := agent["description"].(string); ok {
			entry.Description = value
		}
		if value, ok := agent["enabled"].(bool); ok {
			entry.Enabled = value
		}
		agents = append(agents, entry)
	}
	return connect.NewResponse(&agentcomposev2.GetProjectResponse{Project: &agentcomposev2.Project{
		Summary: &agentcomposev2.ProjectSummary{ProjectId: id},
		Agents:  agents,
	}}), nil
}
