# Bulk Welcome Email After User Import: Batch Sending, Rate Limits, and Retries

Use a durable queue and an auditable delivery ledger when a user import creates more welcome email than an operator can reconcile by hand; otherwise reach for a small, supervised batch whose results are still recorded. A Node.js import process may create the work, but it shouldn't wait for every transactional email to cross an external delivery boundary before it commits the users.

That is the decision.

The governing invariant is one welcome intent per eligible imported account, not one network call and certainly not one optimistic success log. I want every intent to end as submitted, permanently rejected, suppressed, or explicitly uncertain, with enough evidence to explain the transition later. Exactly-once delivery across an external mail system isn't something I assume; exactly-once recording of our decisions is the part I can design.

## How should a Node.js batch send bulk welcome transactional email after a user import?

The import transaction should insert each user and its welcome intent together, using a uniqueness constraint over the import ID, user ID, and template version. Once that transaction commits, a worker claims pending intents under a lease and sends at a controlled concurrency. This decouples account creation from mail latency while preserving a countable obligation: if an import creates 8,412 eligible users, the ledger must eventually explain 8,412 dispositions. A Node.js service can implement the producer and worker, although the critical-path example below is in Go because I use the same storage contract across runtimes. I store the address snapshot, locale, template version, consent or suppression decision, attempt number, lease owner, lease expiry, provider correlation value, and timestamps for every state change. The idempotency key is deterministic and local. A provider-supported key is useful when available, but it doesn't replace the record that tells an operator why the application attempted the message. Nor does a successful submission mean inbox placement or reading; those are different events and should remain different columns. This separation matters during support investigations: a user who didn't see a message may have no intent, a suppressed intent, an unsubmitted intent, a sender-accepted message, or an accepted message that never became observable as delivery, and collapsing those states into `sent=true` destroys the evidence needed to distinguish them.

I learned this boundary the expensive way. In one ledger-adjacent notification job, the call returned 200, the side effect never happened, and reconciliation exposed the gap 7 hours later. I'm not sure why the downstream system accepted the request without producing the action, but I do know our mistake: we had treated transport success as business completion. Since then, I don't allow an HTTP status to close an audit trail.

Keep the state machine small. `pending` may become `leased`; `leased` may become `submitted`, `retryable`, `rejected`, or `uncertain`; only a reconciler or an operator should resolve `uncertain`. If a worker loses its lease after calling the sender but before recording the response, blindly retrying can duplicate the welcome. Preserve that ambiguity — uncomfortable as it is — and reconcile against correlation evidence before another submission.

## What are the failure boundaries for rate limits and retries?

Rate limiting is a scheduling signal, not a generic failure bucket. When the selected sender documents a retry delay, the worker should honor it; otherwise I use exponential backoff with jitter, a maximum attempt count, and an absolute retry horizon. The batch scheduler also needs a global concurrency ceiling, because fifty workers independently behaving politely can still produce an impolite aggregate. Your mileage may vary on the exact numbers, so make them configuration backed by load tests and the sender's published limits rather than constants copied from another system.

Permanent rejection needs a separate path. Invalid message data, an ineligible recipient, or a policy decision shouldn't circulate through the retry queue. Network ambiguity deserves more care: if the request may have crossed the boundary, mark the attempt uncertain and reconcile it instead of converting missing evidence into another send. This is the payment-system habit I bring to email: retries are financial-like decisions, even when the payload is only a welcome note.

| Delivery pattern | Failure boundary | Audit burden | Appropriate use |
| --- | --- | --- | --- |
| Synchronous loop in the import request | Process lifetime and request timeout | High once the request ends | Tiny supervised imports or local prototypes |
| Durable queue with leased workers | Lease expiry and sender acceptance | Moderate; each transition is persisted | Repeatable imports with controlled throughput |
| Database outbox plus queue | Database commit, relay, then sender acceptance | Higher, but intent creation is atomic with user creation | Imports where missing one welcome intent is unacceptable |
| Existing managed relay | Relay acceptance and its event boundary | Depends on retained correlation evidence | Organizations whose messaging team already owns delivery controls |

Sender authentication and responsible sending practices belong in the design review, not in a launch-week checklist; Google's email sender guidelines are the relevant primary reference for mail sent to Gmail accounts. For regulated systems, retention is another boundary. Don't put full message bodies or raw addresses into broadly searchable logs, and have counsel or compliance owners approve how long delivery evidence is retained. An immutable audit event can carry an internal recipient key and a template hash without becoming a shadow customer database.

## What does the idempotent critical path look like?

The useful abstraction has three pieces: a store that atomically claims one intent, a sender that returns classified evidence, and a policy that schedules only documented temporary outcomes. `Claim` must prevent two workers from owning the same live lease. `RecordSubmitted` and `RecordRetry` must compare the lease token before changing state, so a late worker can't overwrite a newer decision. In a Node.js implementation I would keep these interfaces and transaction semantics, even though the syntax changes.

```go
package welcome

import (
	"context"
	"errors"
	"time"
)

type Intent struct {
	ID             string
	LeaseToken     string
	Recipient      string
	Template       string
	IdempotencyKey string
	Attempt        int
}

type Receipt struct {
	CorrelationID string
	RetryAfter    time.Duration
	Temporary     bool
}

type Store interface {
	Claim(context.Context, time.Duration) (*Intent, error)
	RecordSubmitted(context.Context, string, string, string) error
	RecordRetry(context.Context, string, string, time.Time) error
	RecordRejected(context.Context, string, string, string) error
	RecordUncertain(context.Context, string, string, string) error
}

type Sender interface {
	Send(context.Context, Intent) (Receipt, error)
}

func DeliverOne(ctx context.Context, now time.Time, store Store, sender Sender) error {
	intent, err := store.Claim(ctx, 30*time.Second)
	if err != nil || intent == nil {
		return err
	}

	receipt, err := sender.Send(ctx, *intent)
	if err == nil {
		return store.RecordSubmitted(ctx, intent.ID, intent.LeaseToken, receipt.CorrelationID)
	}
	if receipt.Temporary && intent.Attempt < 6 {
		delay := receipt.RetryAfter
		if delay <= 0 {
			delay = backoff(intent.Attempt)
		}
		return store.RecordRetry(ctx, intent.ID, intent.LeaseToken, now.Add(delay))
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return store.RecordUncertain(ctx, intent.ID, intent.LeaseToken, "submission outcome unknown")
	}
	return store.RecordRejected(ctx, intent.ID, intent.LeaseToken, "non-retryable submission")
}

func backoff(attempt int) time.Duration {
	if attempt > 6 {
		attempt = 6
	}
	return time.Second * time.Duration(1<<attempt)
}
```

The example deliberately leaves the sender adapter out. Its request shape, authentication, retry classification, and receipt parsing must come from the chosen sender's documentation; inventing a generic endpoint would make copyable code less safe. In production I would add randomized jitter, encrypt the address snapshot, and append an audit event inside each store transaction. I would also test the lease-token comparison with two workers, because a happy-path unit test won't expose stale ownership.

## What should testing, deployment, and reconciliation prove?

Test the transition table before polishing the template. I exercise duplicate import rows, concurrent claims, a lease expiring before submission, a temporary throttle with and without a documented delay, a permanent rejection, a deadline after an ambiguous send, and duplicate delivery events. Then I run a canary against controlled recipients and reconcile five independent totals: eligible intents, suppressed intents, pending work, submitted work, and unresolved work. The sum must equal the import's recorded population.

No hand waving.

Operationally, queue depth alone is weak evidence. Track the age of the oldest pending intent, attempts by disposition, lease expirations, uncertain outcomes, and the gap between imported eligible users and terminal records. Deployment needs a pause control that stops new claims without deleting work. Rollback should preserve the ledger schema and events even if the worker binary changes, since destroying evidence to simplify a rollback is the notification equivalent of editing a payment journal.

The email content also deserves a boundary: a welcome transaction should identify the account action and sender clearly, while campaign analytics, audience segmentation, and promotional consent belong to a campaign workflow. If SMS is a fallback, don't reuse the email scheduler blindly. SMS length and encoding affect segmentation, as the referenced character-limit guidance explains, and that changes both user experience and operational accounting.

The catch is that the outbox-and-ledger design isn't suitable for every job. For ten recipients under direct operator supervision, a synchronous script with a saved result file may be easier to inspect; stick with an existing organizational relay when another team already owns authentication, reputation, archival policy, and incident response. A campaign system is the better boundary for promotional onboarding with unsubscribe and audience-management requirements. I reject the synchronous loop for repeatable production imports because process death makes recovery ambiguous, yet it remains valid for a disposable local prototype where nobody mistakes its output for an auditable delivery record.

## References

- https://support.google.com/a/answer/81126
- https://www.twilio.com/docs/glossary/what-sms-character-limit
