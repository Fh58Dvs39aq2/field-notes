# Node.js Transactional Email Templates: Preview Artifacts Anchor Edtech Settlement Receipts

TL;DR: In Node.js, create transactional email templates as versioned artifacts, preview the exact rendering path, and send an edtech order receipt only from a durable payment-settlement intent. The durable record must decide whether to send: one settlement identity creates one receipt intent, every attempt carries the same artifact digest, and remote acceptance is evidence of an attempt rather than proof that the learner received the message.

The decision rule is strict: if the system cannot reconstruct exactly what it intended to send, to whom, and because of which settlement, it is not ready to dispatch. This design makes retries ordinary, template updates non-destructive, and deliverability investigations answerable without pretending that email offers exactly-once delivery.

## How should Node.js create and preview transactional email templates?

The architecture uses a transactional outbox beside the order and payment records, an immutable rendered artifact produced from an approved template version, and an asynchronous dispatcher. The commit that changes a payment to `settled` also inserts a uniquely keyed receipt intent. A worker later renders or retrieves the pinned artifact and attempts delivery. Database uniqueness supplies the exactly-once *decision*; retries provide at-least-once execution outside the database boundary.

One event, one intent.

Four invariants carry most of the correctness burden:

1. A settlement reference can create at most one logical receipt intent for an order.
2. An intent pins the template version, locale, input snapshot, recipient, subject, and content digest; editing a template never rewrites an existing intent.
3. Each dispatch attempt is append-only and records its own correlation identifier, timestamps, outcome class, and remote message identifier when one exists.
4. A delivery or bounce event may advance message state only when its correlation data resolves to the recorded attempt; an unknown event is quarantined, not guessed into an order.

These are ledger-shaped rules because the business event is ledger-shaped. A parent buying a course should not receive two receipts because a queue lease expired, nor should a later branding edit silently alter the evidence associated with an earlier purchase. Keep the two domains separate: payment settlement authorizes creation of the intent, while email infrastructure executes and observes attempts.

This design has a real limitation: immutability adds artifact storage, an approval transition, and operational reconciliation. The trade-off is justified for a payment receipt because historical consistency matters more than last-minute copy changes; it does not fit an informal classroom reminder whose author expects to revise queued wording until dispatch.

The compliance boundary belongs in the data model as well. Retention periods, deletion obligations, access controls, and whether message bodies may be stored depend on jurisdiction and institutional policy, so the architecture should support body redaction and independently retained digests rather than inventing a universal retention number. A digest proves byte equality against a retained artifact; it does not prove inbox placement, authorship on its own, or legal compliance.

## Where can a settled-payment receipt fail?

Failure can occur before rendering, during dispatch, after remote acceptance, or while processing feedback. The distinctions matter. A malformed input is deterministic and should stop retries; a timeout after submission is ambiguous because the remote system may have accepted the message even though the worker did not observe the response. A hard bounce is neither of those: it is later evidence about the destination.

Never collapse them into one `failed` flag.

Ambiguity is a state.

The dangerous interval is the network call. No local transaction can atomically commit a database row and force an independent mail system to accept exactly one message. If the worker crashes after remote acceptance but before recording that acceptance, a retry may submit again. An idempotency key understood across the dispatch boundary can reduce duplicates; absent that contract, the system must acknowledge the residual ambiguity, preserve attempt evidence, and reconcile late callbacks rather than claim exactly-once delivery.

Authentication has a different failure boundary. DKIM signs selected header fields and the message body so a verifier can validate a responsible signing domain and detect modification of signed content in transit. RFC 6376 also makes clear that DKIM does not itself prescribe message filtering policy. Therefore a valid signature belongs in the deliverability baseline, but it is not a receipt guarantee. Sender authorization, domain alignment policy, list hygiene, complaint handling, and content consistency remain operational concerns outside the template renderer.

## Evidence model and option comparison

The template lifecycle has three states worth keeping distinct: editable source, approved immutable version, and rendered receipt artifact. Preview should render the same approved version through the same escaping, localization, and MIME assembly path used by dispatch. A browser-only mock catches visual mistakes; it does not establish parity with the bytes handed to the transport.

| Option | Retry behavior | Audit evidence | Update behavior | Appropriate boundary |
| --- | --- | --- | --- | --- |
| Render current template inside the request | Coupled to payment latency | Weak unless the full result is retained | Old receipts can change on retry | Low-consequence notifications where reconstruction is unnecessary |
| Queue variables, render latest at send time | Queue absorbs transient outages | Input is known, output may drift | A release can alter already queued mail | Messages whose wording may intentionally follow the newest policy |
| Pin version and immutable artifact | Retry reuses identical content | Intent, digest, and attempts form a chain | New versions affect only new intents | Financially relevant order receipts |

The third option is the decision here. It consumes storage and requires an approval transition, but those costs buy a stable answer to a practical dispute: “What receipt did the system send after this settlement?” Store normalized inputs as well as the final artifact when policy permits, because the pair separates a rendering defect from bad upstream data. Encrypt sensitive fields, restrict operator access, and log reads of receipt evidence. An audit table that everyone can casually query is not a control.

That extra machinery is unnecessary when a message has no durable business consequence and no reconstruction requirement. In that case, rendering at execution time is easier to operate, and choosing the immutable path would be needless complexity.

Template updates follow a narrow path. Create a new version, render deterministic fixtures for supported locales, inspect both text and HTML alternatives, validate required variables, approve the digest, then allow new intents to reference it. Preview artifacts should include edge cases such as a long course title, a missing optional tax label, escaped learner-supplied text, and a currency amount represented in minor units. Do not use floating-point arithmetic to reconstruct payment totals in the email path.

## Critical path: commit, claim, and record

The following Go sketch shows the contract even when the surrounding application is Node.js: the database operation and transport are interfaces, while uniqueness and immutable payloads define the behavior. The same boundaries map directly to a Node.js transaction and worker without tying the design to a mail product.

```go
package receipt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Intent struct {
	ID, OrderID, SettlementID string
	Recipient, TemplateVersion string
	Subject, MIMEBody, Digest  string
}

type Attempt struct {
	IntentID, AttemptID, Outcome, RemoteID string
	StartedAt, FinishedAt                 time.Time
}

type Store interface {
	InsertSettlementAndIntent(ctx context.Context, in Intent) error // one transaction
	ClaimPendingIntent(ctx context.Context) (Intent, error)
	AppendAttempt(ctx context.Context, attempt Attempt) error
}

type Transport interface {
	Send(ctx context.Context, idempotencyKey, recipient, subject, mimeBody string) (string, error)
}

func Digest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func Dispatch(ctx context.Context, db Store, mail Transport, now func() time.Time) error {
	in, err := db.ClaimPendingIntent(ctx)
	if err != nil {
		return err
	}
	if Digest(in.MIMEBody) != in.Digest {
		return errors.New("receipt artifact digest mismatch")
	}

	started := now()
	remoteID, sendErr := mail.Send(ctx, in.ID, in.Recipient, in.Subject, in.MIMEBody)
	outcome := "accepted"
	if sendErr != nil {
		outcome = "ambiguous_or_rejected"
	}
	recordErr := db.AppendAttempt(ctx, Attempt{
		IntentID: in.ID, AttemptID: in.ID + ":" + started.UTC().Format(time.RFC3339Nano),
		Outcome: outcome, RemoteID: remoteID, StartedAt: started, FinishedAt: now(),
	})
	if recordErr != nil {
		return recordErr
	}
	return sendErr
}
```

The abbreviated outcome above should become a typed taxonomy in production: local validation rejection, remote rejection, explicit remote acceptance, and unknown result after timeout need different retry and reconciliation rules. The worker must also claim with a lease or row lock, apply bounded backoff, and move exhausted work to an operator-visible state. Those mechanisms prevent hot loops; they do not erase ambiguous acceptance.

Observability should join the same identities without exposing message bodies. Useful counters include intents created, render validation failures, attempts by outcome class, callback events that cannot be correlated, and intent age. Trace the order ID, settlement ID, intent ID, and attempt ID through the Node.js ingress, queue, renderer, and callback consumer. Recipient addresses do not belong in metric labels.

Deployment is safest when schema and workers tolerate both the previous and next template-version formats during rollout. Publish the immutable artifact before enabling its version, run fixture previews in continuous integration, and use a small initial dispatch cohort for a new version. Rollback then means disabling new references to that version; it does not mutate receipts already authorized.

## Rejected option and the case where it fits

Rendering the latest template at worker execution time was rejected because queue delay would turn a template release into an unrecorded change to already authorized receipts. It also makes a retry capable of producing different bytes from the first attempt, weakening reconciliation precisely when an operator needs reliable evidence.

That model still has a valid use case. A school-closure bulletin queued before administrators finalize wording may be expected to use the latest approved copy at dispatch, provided the event is not a financial receipt and the system records which version ultimately rendered. The choice follows the semantic promise, not a blanket rule that every email must be immutable.

For settled-payment receipts, keep the promise narrow: commit one intent with the settlement, preview the real renderer, pin the approved artifact, append every attempt, and reconcile feedback against recorded identifiers. That architecture cannot guarantee an inbox, because the receiving domain owns the last part of delivery. It can guarantee something the backend actually controls: consistent content, explicit uncertainty, and an audit trail that survives retries and template releases.

## References

- https://datatracker.ietf.org/doc/html/rfc6376
