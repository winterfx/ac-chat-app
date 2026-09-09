# chatui

A browser chat UI for agent-compose agents, built on the
[Go chat SDK](../sdk/go).

```bash
go run . -daemon http://127.0.0.1:7410
# http://127.0.0.1:7500
```

With no accounts yet, the first start creates `admin` and prints a generated
password once. `AGENT_COMPOSE_AUTH_TOKEN` is sent to the daemon as a bearer
token when set.

```bash
go run . -add-user alice        # prompts for a password, no echo
go run . -set-password alice
```

| flag | default | |
|---|---|---|
| `-listen` | `127.0.0.1:7500` | where to serve the UI |
| `-daemon` | `http://127.0.0.1:7411` | the agent-compose daemon's HTTP address |
| `-state` | `~/.ac-chat-app/state.json` | accounts and the chat list |
| `-allow-origin` | | extra origins allowed to open a chat socket |
| `-idle-after` | `15m` | end a conversation's run once nobody has watched it for this long |
| `-sweep-every` | `1m` | how often to look for conversations that have gone idle |

`-idle-after` is the knob for how many environments this server keeps warm: a
shorter window frees sandboxes sooner and makes the next message pay for a
restart, a longer one keeps a conversation instant to return to.

For local development, copy `.env.example` to `.env` and set the daemon
address. The app reads `AGENT_COMPOSE_AUTH_TOKEN` when the daemon's HTTP(S)
control plane requires a bearer token; an empty value means token
authentication is disabled on the daemon.

```env
AC_DAEMON=http://127.0.0.1:7410
AC_LISTEN=127.0.0.1:7500
AGENT_COMPOSE_AUTH_TOKEN=your-token
```

## Why a server sits in the middle

A browser cannot hold a conversation with the daemon directly. Connect needs
HTTP/2 bidirectional streaming for a multi-turn session, and a browser
`fetch()` with a streaming request body is **half duplex** — no response bytes
arrive while the request body is still open, which is exactly the state a
multi-turn session lives in.

So the conversation stays on the Go side, where the SDK handles the
bidirectional stream. The browser keeps a duplex connection of its own, in the
shape it can actually open:

```
browser ──WebSocket──> chatui ──Connect bidi (h2c)──> daemon
```

Duplex the whole way, because a conversation is duplex the whole way. The
browser still never encodes a protocol frame, never threads a sandbox ID
between turns, and never tracks session lifetime — those belong to the SDK.

Keeping the browser connected, rather than streaming one turn per request,
is what makes the rest work:

- **A turn nobody here started still shows up.** Once a conversation outlives
  the page that opened it, the turn in flight is often one this browser did not
  send — from a phone, or from before a refresh. Viewers render from the socket,
  not from what they just typed.
- **Several viewers share one conversation.** A laptop and a phone on the same
  conversation both watch the same turn live.
- **Leaving is a signal.** A closed socket is how this server learns nobody is
  watching, which is what lets it release the conversation.

There is no fan-out machinery behind this: every viewer ranges over the same
`chat.Reply`, which retains its events and replays them for each caller, so a
viewer joining mid-turn sees that turn from its beginning.

## What it does

- Signs in, and shows each person only their own conversations
- Lists past conversations by recency, with rename and delete
- Shows the connected daemon address, then picks a project and one of its
  available agents for a multi-turn conversation
- Shows what the agent is doing as it works: a collapsible trace of steps, tool
  calls and their output, reasoning, plans, retries, and token usage
- Survives a restart of this server: conversations are resumed by ID, and the
  agent keeps the context it built up
- Says so when a conversation's environment was rebuilt, rather than silently
  starting over with no context
- Reads back history, including turns that ran while nobody was watching

## The activity trace

A turn renders as two things: what the agent **did**, and what it **said**. The
trace above each answer holds the first, folded away once the turn finishes
unless the reader opened it themselves.

Work is grouped by the step number each event carries — never by what falls
between a `step_start` and a `step_end`. Providers interleave, so an event's own
step number is the only thing that says where it belongs. Events from a provider
that does not number its steps land in one shared panel rather than being
assigned a step they never claimed. A run-scoped `step_end` closes the turn
rather than a step and creates no panel of its own.

Token usage is shown per scope and **never summed across scopes**: one provider
reports a step's tokens, another repeats a running total for the whole turn, and
adding those together produces a number that means nothing. Step-scoped records
cover disjoint work and do add up; turn- and run-scoped records replace the
previous total.

A field a provider did not report is omitted rather than sent as a zero, so the
page can tell "did not happen" from "this provider never reports it".

## Where the chat list lives

Identity is on the daemon; the index is here.

Every run this server starts carries three labels:

| | |
|---|---|
| `chat.conversation` | the conversation's identity — what `Agent.Open` resumes |
| `chat.user` | who it belongs to |
| `chat.app` | that this app started it |

**Ownership is answered by those labels, in one call and with nothing read
back.** `ListRuns` filtered on `{chat.conversation, chat.user}` returning a run
*is* the proof the conversation is that user's; an empty result is the proof it
is not. So a conversation resolves for its owner, and hides from everyone else,
even when this server has no record of it.

**The list is not**, because a run summary carries no labels — they belong to a
run's detail, so grouping a page of runs into conversations costs one `GetRun`
per run. The sidebar is redrawn after every turn, so it reads a local index
instead: `state.json` maps each conversation to its owner, its agent, and its
title.

Losing that file therefore loses the *list*, not the conversations. The ⟳ button
rebuilds it from the daemon — the expensive path, run when asked rather than on
every page load. Recovered conversations come back without titles, because a
title was never the daemon's to hold: labels are fixed when a run starts, and a
rename has to work without starting one.

## Accounts

Accounts live in the state file (`-state`, by default
`~/.agent-compose/chatui/state.json`). Passwords are stored as
PBKDF2-HMAC-SHA256 verifiers, never in the clear. Sessions are cookie-based and
live in memory, so restarting the server signs everyone out.

Each conversation belongs to one account, and ownership is enforced from the
`chat.user` label rather than from this server's own record — so it holds even
for a conversation this process has never seen, and a tampered-with state file
cannot grant access the daemon would refuse. Asking for someone else's is
answered as *not found* rather than *forbidden*: that another user has a
conversation is itself none of the asker's business.

This is a sign-in, not an identity system: there are no roles, no sharing, and
no password reset beyond `-set-password`. Put it behind TLS before exposing it
past localhost — the session cookie is marked `Secure` only when the request
arrives over HTTPS.

## HTTP API

Discrete actions stay plain HTTP; one authenticated browser socket carries the
conversation streams the page subscribes to.
Everything except `POST /api/login` and the page requires a session.

| | |
|---|---|
| `POST /api/login` | `{user, password}` → session cookie |
| `POST /api/logout` | drop the session |
| `GET /api/me` | who is signed in, or `null` |
| `GET /api/agents` | `{daemon, projects}`: the connected daemon address and each project grouped with its agents; unavailable agents are marked non-selectable |
| `GET /api/conversations` | this user's chat list, most recent first |
| `POST /api/conversations/recover` | rebuild the list from the daemon |
| `POST /api/conversations` | `{projectId, agentName}` → a new conversation |
| `GET /api/conversations/{id}` | one conversation and its transcript |
| `PATCH /api/conversations/{id}` | `{title}` |
| `DELETE /api/conversations/{id}` | end the conversation's session (stopping its environment) and drop it from this list |
| `GET /api/socket` | one authenticated WebSocket for multiple conversations |

On the socket, the browser first sends `{type:"subscribe", conversationId}`.
It may subscribe to more than one conversation, and sends
`{type:"message", conversationId, text}`, `{type:"stop", conversationId}`, or
`{type:"unsubscribe", conversationId}`. Every conversation-specific response
includes `conversationId`, followed by `turn_started` (with the prompt, so a
viewer that did not send it sees the question), `event` (one SDK event),
`turn_done`, `conversation` (the updated sidebar row), `stopped`, and `error`.
`subscribed` and `unsubscribed` acknowledge subscription changes.

Each conversation on a socket is handled on its own, so one waiting on the
daemon does not hold up the frames of another; within a conversation, frames
keep the order they arrived in. Subscribing to a conversation the socket
already holds is acknowledged again rather than ignored, because a page that
reconnects cannot always tell which it is.

Subscribing picks up a turn that is still running, from its first event — the
normal case once a conversation outlives the page that started it. A turn that
has already finished is not replayed: it is in the transcript
`GET /api/conversations/{id}` returns, and sending it again would draw the same
exchange twice.

A handshake is accepted only from the page this server serves; `-allow-origin`
adds others. A WebSocket handshake is not subject to the same-origin policy,
so a permissive check would let any site a viewer visits drive their
conversations.

Attached conversations live in this process's memory, and one nobody has
watched for `-idle-after` (fifteen minutes by default) has its run ended, which
is what stops the sandbox behind it. That is not ending the conversation: it
survives on the daemon and is reopened by ID on the next visit, resuming the
same sandbox with the agent's context intact.

Ending the run is the whole point. A conversation attaches with the daemon's
detach policy, so a turn survives the browser that started it — and so a
dropped stream leaves the run, and its sandbox, running. Nothing on the daemon
side expires them, so this server has to. Shutting down does the same for every
conversation it still holds; a process that is killed outright leaves them
behind, and finding those again means going through the daemon's run labels.

## Limits

- **Stop ends the conversation's run, not just the turn.** The next message
  starts a new run and asks to resume the same sandbox. The UI reports whether
  that preserved the environment or had to rebuild it.
- **The chat list is an index, not a source of truth.** It can drift from the
  daemon — a conversation deleted by another client stays in the sidebar until
  it is opened. Rebuilding it is one click, not automatic, because the daemon
  answers it at one `GetRun` per run.
- **History is read in pages.** Opening a conversation uses the SDK's
  `HistoryPage` cursor to load durable user and assistant messages without
  imposing a 200-run limit on long conversations.
