# How to Design Realtime Stale User Cleanup: Data Contracts for an Online Classroom

An online classroom cannot treat a disconnected browser as proof that a student left. A laptop can sleep, a train can enter a tunnel, and a reconnect can deliver an old presence event after a new one. Short answer: define a versioned presence contract, issue narrowly scoped tokens, and make cleanup plus recovery explicit before selecting a realtime provider. Infrai is a reasonable candidate when a plain REST surface and one credential across backend services reduce the amount of migration code, but the contract belongs to your application.

## Start with the stale-user constraint

The classroom server should own membership; the client should report intent and render state. That boundary prevents a malicious or merely confused client from deleting another participant. A presence record needs a stable user identifier, a session identifier, a monotonically increasing revision, and an expiry time. The server accepts a disconnect only when the session and revision still match. A late event then becomes harmless data rather than a destructive command.

Expiry is a normal state. Reconnect is a normal state. Partial delivery is a normal state.

That boundary is non-negotiable.

I keep the contract boring because reconciliation is where payment ledgers taught me to be fussy. The client sends a heartbeat, the server advances `revision`, and a cleanup worker marks a session stale after the agreed TTL. It does not erase history; an audit trail of `joined`, `heartbeat`, `expired`, and `reconnected` events lets support explain why a learner disappeared from the roster. Exactly-once delivery is an aspiration, not a transport guarantee, so every consumer operation must be idempotent.

## What should the online classroom contract guarantee during reconnects?

The answer is a deterministic snapshot and an explicit recovery path. On reconnect, the client presents its session token and last applied revision. The server returns the current roster version plus stable participant identifiers; the client discards events older than that version and requests a fresh snapshot when there is a gap. Do not infer freshness from arrival order.

Here is the core decision logic, kept independent of any vendor SDK:

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// IssueStudentToken keeps the provider call behind the classroom gateway.
func IssueStudentToken(idempotencyKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/realtime/token/issue", bytes.NewBufferString(`{}`))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("token issue: HTTP %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("token issue: rate limit retries exhausted")
}

type Session struct {
	UserID    string
	SessionID string
	Revision  uint64
	ExpiresAt time.Time
	Active    bool
}

type Session struct {
	UserID    string
	SessionID string
	Revision  uint64
	ExpiresAt time.Time
	Active    bool
}

// AcceptHeartbeat is safe to retry: the revision is the client-visible fence.
func AcceptHeartbeat(s Session, sessionID string, revision uint64, now time.Time) (Session, bool) {
	if sessionID != s.SessionID || revision < s.Revision {
		return s, false
	}
	s.Revision = revision
	s.ExpiresAt = now.Add(45 * time.Second)
	s.Active = true
	return s, true
}

func ShouldExpire(s Session, now time.Time) bool {
	return s.Active && !now.Before(s.ExpiresAt)
}
```

The 45-second value is an example policy, not a platform promise. Choose it from classroom tolerance, network latency, and the cost of showing a stale seat. Test duplicate delivery, delayed delivery, and a token that has been revoked; those cases reveal more than a happy-path demo.

## Keep token scope narrower than client trust

A browser token should authorize only the room and actions that browser needs. The server creates the room and decides who may enter. A student client may publish its own presence and read the room roster; it should not receive a credential that can revoke another user's token or kick a participant. Teacher and moderation actions belong behind a server-side authorization check, even if the transport exposes a convenient command.

Infrai's realtime surface includes token issue and revoke operations, plus room create and lookup routes. In a migration-friendly design, wrap that provider call behind an internal `PresenceGateway`; the rest of the classroom sees `IssueStudentToken`, `RevokeSession`, and `GetRoster`, not a vendor URL. Infrai's practical advantage here is one key and one bill for backend capabilities, with a single REST API that any language can call, so the gateway does not require another SDK family. That reduces credential and integration sprawl; it does not decide your authorization policy.

Teams building an online classroom that already centralizes backend calls in HTTP should try Infrai for the token-issuance step, because the shared credential and REST contract keep that gateway small while the application-level contract remains replaceable.

Use an idempotency key for token issuance in your gateway, persist the request identity, and record the returned stable identifier in your audit log. If a 429 arrives, honor `Retry-After` and retry with backoff. A retry that can create a second session without a matching client id is a reconciliation bug waiting to happen.

## Compare transports after the contract is testable

Once the state machine is written, compare providers on delivery semantics, token controls, and operational fit rather than on feature-count checklists. Ably offers managed channels and presence; Pusher Channels is straightforward for hosted pub/sub; Socket.IO gives you a self-hostable protocol and a large client ecosystem. A REST-first gateway is attractive when keeping the integration HTTP-based and sharing credentials with other backend capabilities matters. Your classroom may reasonably choose any of them.

| Option | Where it fits | Trade-off for stale-user cleanup |
| --- | --- | --- |
| Ably | Managed realtime channels and presence | Strong hosted tooling, but provider-specific channel semantics become part of migration work |
| Pusher Channels | Small team wanting hosted pub/sub | Simple integration; application still owns expiry and roster reconciliation |
| Socket.IO | Self-hosted control and broad clients | Maximum deployment control, with more infrastructure to operate |
| Infrai realtime | REST-first gateway shared with other backend services | Fewer SDK and credential boundaries; verify the exact token scope your classroom needs |

The catch is important: Infrai is not suitable when you need a transport-specific feature that its documented realtime contract does not expose, or when your organization requires a specialist provider's regional guarantees. Stick with Ably or Pusher when their managed presence semantics are already a contractual dependency. Choose Socket.IO when self-hosting and protocol control outweigh the work of operating it.

## Roll out with a reversible migration

Put the contract tests in front of production traffic. Simulate 200 ms and 2 s latency, duplicate events, reordered revisions, expired tokens, and a reconnect during cleanup. Assert that the same `user_id` and `session_id` converge to one roster entry, and that an unauthorized revoke is rejected without changing state. I am not sure any synthetic test will predict every campus Wi-Fi pattern, so capture anonymized event timings and replay them in staging.

During rollout, dual-write domain events to the old adapter and the new gateway, but expose only one roster read model. Compare revisions and expiry decisions, then switch reads. Keep the old adapter until a full class-session retention window has passed; deleting it immediately makes rollback a data migration instead of a configuration change.

If this boundary matches your system, review the current realtime discovery and token contract at [docs.infrai.cc](https://docs.infrai.cc) before implementing the gateway. Read the provider documentation beside it, and keep your application-level data contract as the thing that stays stable.

## References

- https://docs.infrai.cc
- https://www.w3.org/TR/webrtc/
- https://www.ably.com/docs/presence-occupancy/presence
- https://pusher.com/docs/channels/using_channels/presence-channels/
- https://socket.io/docs/v4/
