package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/chaitin/agent-compose/sdk/go/chat"
)

const (
	// writeWait bounds a single frame write.
	writeWait = 10 * time.Second
	// pongWait is how long a viewer may go silent before it is dropped, and
	// pingPeriod how often it is prodded. Proxies close idle connections, and a
	// conversation is idle most of the time.
	pongWait   = 60 * time.Second
	pingPeriod = 25 * time.Second
	// pendingFrames is how many frames one conversation may have waiting on
	// its own worker. Beyond that the reader blocks, which is backpressure on
	// the conversation that is behind rather than on the whole socket.
	pendingFrames = 16
	// maxSocketConversations bounds how many conversations one connection may
	// name. Each costs a goroutine and a queue, and they are created before the
	// conversation is known to exist, so without a limit a signed-in client
	// could grow them by naming IDs at random. A page watches a handful.
	maxSocketConversations = 64
)

// clientFrame is what the browser sends.
type clientFrame struct {
	Type           string `json:"type"`
	ConversationID string `json:"conversationId,omitempty"`
	Text           string `json:"text,omitempty"`
}

type socketSubscription struct {
	id     string
	sess   *session
	cancel context.CancelFunc
}

type socketConnection struct {
	server   *uiServer
	conn     *websocket.Conn
	user     string
	ctx      context.Context
	cancel   context.CancelFunc
	outbound chan any

	mu            sync.Mutex
	subscriptions map[string]*socketSubscription
	// queues gives every conversation its own serial worker, so a frame that
	// has to wait on the daemon holds up only its own conversation.
	queues  map[string]chan clientFrame
	workers sync.WaitGroup
}

// socket serves one authenticated browser connection. Each connection can
// subscribe to any number of conversations; every subscription still owns its
// own SDK Conversation and daemon stream. Events are tagged with the
// conversation ID before they enter the shared WebSocket writer.
//
// The browser and this server hold one multiplexed control channel; each
// subscription then owns the SDK's duplex stream to the daemon. This keeps
// browser transport fan-in separate from conversation lifecycle and lets one
// connection observe several independent conversations.
func (s *uiServer) socket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written a response.
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	socket := &socketConnection{
		server: s, conn: conn, user: userOf(r), ctx: ctx, cancel: cancel,
		outbound:      make(chan any, 128),
		subscriptions: make(map[string]*socketSubscription),
		queues:        make(map[string]chan clientFrame),
	}
	go writeFrames(ctx, cancel, conn, socket.outbound)
	// Order matters on the way out: the workers are stopped and drained before
	// subscriptions are released, so one that was mid-attach cannot register a
	// viewer after the cleanup that was supposed to release it.
	defer func() {
		cancel()
		socket.workers.Wait()
		socket.closeSubscriptions()
		_ = conn.Close()
	}()

	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		var frame clientFrame
		if err := conn.ReadJSON(&frame); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Debug("chat socket ended", "error", err)
			}
			return
		}
		socket.dispatch(frame)
	}
}

// dispatch routes a frame to the worker that owns its conversation.
//
// Every frame type below can block on the daemon — subscribing opens a stream,
// a message is a round trip — so handling them on the read loop would make one
// slow conversation freeze every other one on the same socket, which is the
// whole point of sharing it. Frames for one conversation still run in the
// order they arrived, so a message can never overtake the subscribe that has
// to precede it.
func (c *socketConnection) dispatch(frame clientFrame) {
	switch frame.Type {
	case "subscribe", "unsubscribe", "message", "stop":
	default:
		c.sendError(strings.TrimSpace(frame.ConversationID), errors.New("unknown socket frame type"))
		return
	}
	id := strings.TrimSpace(frame.ConversationID)
	if id == "" {
		c.sendError("", errors.New("conversationId is required"))
		return
	}
	frame.ConversationID = id

	c.mu.Lock()
	queue, ok := c.queues[id]
	if !ok {
		if len(c.queues) >= maxSocketConversations {
			c.mu.Unlock()
			c.sendError(id, errors.New("too many conversations on one connection"))
			return
		}
		queue = make(chan clientFrame, pendingFrames)
		c.queues[id] = queue
		c.workers.Add(1)
		go c.serve(queue)
	}
	c.mu.Unlock()

	select {
	case queue <- frame:
	case <-c.ctx.Done():
	}
}

// serve runs one conversation's frames, one at a time.
func (c *socketConnection) serve(queue <-chan clientFrame) {
	defer c.workers.Done()
	for {
		select {
		case frame := <-queue:
			c.handle(frame)
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *socketConnection) handle(frame clientFrame) {
	id := frame.ConversationID
	switch frame.Type {
	case "subscribe":
		if err := c.subscribe(id); err != nil {
			c.sendError(id, err)
		}
	case "unsubscribe":
		c.unsubscribe(id)
	case "message":
		sub := c.subscription(id)
		if sub == nil {
			c.sendError(id, errors.New("conversation is not subscribed"))
			return
		}
		if err := sub.sess.send(c.ctx, frame.Text); err != nil {
			// The run is what the daemon knows this conversation as, and a
			// send that failed is exactly when someone goes looking there.
			slog.Warn("chat message not sent", "conversation", id, "run", sub.sess.runID(), "error", err)
			c.send(id, map[string]any{"type": "error", "message": err.Error(), "busy": errors.Is(err, chat.ErrBusy)})
			return
		}
		if err := c.server.store.touch(sub.sess.record.Owner, id, frame.Text); err != nil {
			slog.Warn("chat list not updated", "conversation", id, "error", err)
			return
		}
		if updated, err := c.server.store.conversation(sub.sess.record.Owner, id); err == nil {
			c.send(id, map[string]any{"type": "conversation", "conversation": c.server.describeRecord(updated)})
		}
	case "stop":
		sub := c.subscription(id)
		if sub == nil {
			c.sendError(id, errors.New("conversation is not subscribed"))
			return
		}
		stopped, err := sub.sess.interrupt(c.ctx)
		if err != nil {
			c.sendError(id, err)
			return
		}
		c.send(id, map[string]any{"type": "stopped", "stopped": stopped})
	}
}

// subscribe is idempotent, and always answers. A browser that reconnects or
// re-opens a conversation cannot always know whether this socket already holds
// the subscription, so a duplicate is acked rather than dropped: silence would
// leave the page waiting for a confirmation that is never coming.
func (c *socketConnection) subscribe(id string) error {
	c.mu.Lock()
	existing, held := c.subscriptions[id]
	c.mu.Unlock()
	if held {
		c.send(id, map[string]any{"type": "subscribed", "conversation": c.server.describeRecord(existing.sess.record)})
		return nil
	}

	record, err := c.server.findRecord(c.ctx, c.user, id)
	if err != nil {
		return err
	}
	found, err := c.server.attach(c.ctx, record)
	if err != nil {
		return err
	}
	subCtx, subCancel := context.WithCancel(c.ctx)
	sub := &socketSubscription{id: id, sess: found, cancel: subCancel}
	found.join()
	c.mu.Lock()
	if _, ok := c.subscriptions[id]; ok {
		c.mu.Unlock()
		subCancel()
		found.leave()
		c.send(id, map[string]any{"type": "subscribed", "conversation": c.server.describeRecord(record)})
		return nil
	}
	c.subscriptions[id] = sub
	c.mu.Unlock()
	c.send(id, map[string]any{"type": "subscribed", "conversation": c.server.describeRecord(record)})
	go streamTurns(subCtx, found, func(frame any) bool { return c.sendContext(subCtx, id, frame) })
	return nil
}

func (c *socketConnection) unsubscribe(id string) {
	c.mu.Lock()
	sub := c.subscriptions[id]
	delete(c.subscriptions, id)
	c.mu.Unlock()
	if sub == nil {
		return
	}
	sub.cancel()
	sub.sess.leave()
	c.send(id, map[string]any{"type": "unsubscribed"})
}

func (c *socketConnection) subscription(id string) *socketSubscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscriptions[id]
}

func (c *socketConnection) closeSubscriptions() {
	c.mu.Lock()
	subs := make([]*socketSubscription, 0, len(c.subscriptions))
	for id, sub := range c.subscriptions {
		subs = append(subs, sub)
		delete(c.subscriptions, id)
	}
	c.mu.Unlock()
	for _, sub := range subs {
		sub.cancel()
		sub.sess.leave()
	}
}

func (c *socketConnection) send(id string, frame any) bool {
	return c.sendContext(c.ctx, id, frame)
}

// sendContext queues a frame only while both the socket and its owner are
// alive. A subscription has its own context, so unsubscribing one conversation
// cannot leak a cancellation error or a late event onto the shared socket.
func (c *socketConnection) sendContext(ctx context.Context, id string, frame any) bool {
	select {
	case <-ctx.Done():
		return false
	case <-c.ctx.Done():
		return false
	default:
	}
	if id != "" {
		if payload, ok := frame.(map[string]any); ok {
			tagged := make(map[string]any, len(payload)+1)
			for key, value := range payload {
				tagged[key] = value
			}
			tagged["conversationId"] = id
			frame = tagged
		}
	}
	select {
	case c.outbound <- frame:
		return true
	case <-ctx.Done():
		return false
	case <-c.ctx.Done():
		return false
	}
}

func (c *socketConnection) sendError(id string, err error) {
	c.send(id, map[string]any{"type": "error", "message": err.Error()})
}

// streamTurns relays every turn on the conversation, including turns this
// viewer did not start and one already in flight when it connected.
func streamTurns(ctx context.Context, found *session, emit func(any) bool) {
	for reply, prompt := range found.turns(ctx) {
		if !emit(map[string]any{"type": "turn_started", "prompt": prompt}) {
			return
		}
		for event, err := range reply.Events(ctx) {
			if err != nil {
				// Events and Wait report the same failure — both read the
				// reply's single error — so reporting it here as well showed
				// the reader the same message twice. Wait is the turn's
				// authoritative terminal result, so it does the reporting.
				break
			}
			if !emit(map[string]any{"type": "event", "event": renderEvent(event)}) {
				return
			}
		}
		message, err := reply.Wait(ctx)
		if err != nil {
			if !emit(map[string]any{"type": "error", "message": err.Error()}) {
				return
			}
			continue
		}
		if !emit(map[string]any{
			"type":       "turn_done",
			"text":       message.Text,
			"result":     json.RawMessage(cmp.Or(string(message.Result), "null")),
			"continuity": found.conversation.Continuity(),
		}) {
			return
		}
	}
}

// writeFrames owns the connection's write side and keeps it alive through
// proxies that close idle connections.
func writeFrames(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, outbound <-chan any) {
	defer cancel()
	ping := time.NewTicker(pingPeriod)
	defer ping.Stop()
	for {
		select {
		case frame := <-outbound:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteJSON(frame); err != nil {
				return
			}
		case <-ping.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-ctx.Done():
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		}
	}
}

// sameOriginOnly accepts a handshake only from the page this server serves.
//
// A WebSocket handshake is not subject to the same-origin policy, so a
// permissive check would let any site a viewer visits drive their
// conversations. allowed adds explicit origins for a separately hosted page.
func sameOriginOnly(allowed []string) func(*http.Request) bool {
	permitted := make(map[string]struct{}, len(allowed))
	for _, origin := range allowed {
		if origin = strings.TrimSpace(origin); origin != "" {
			permitted[strings.ToLower(origin)] = struct{}{}
		}
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			// Not a browser; there is no origin to lie about.
			return true
		}
		if _, ok := permitted[strings.ToLower(origin)]; ok {
			return true
		}
		parsed, err := url.Parse(origin)
		return err == nil && strings.EqualFold(parsed.Host, r.Host)
	}
}
