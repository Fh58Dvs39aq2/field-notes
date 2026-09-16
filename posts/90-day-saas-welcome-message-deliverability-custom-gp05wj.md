# 90-Day SaaS Welcome Message Deliverability — Custom Domain DKIM Evidence

For SaaS welcome email deliverability, the checklist should start with a controlled custom sending domain, DKIM, and a suppression decision made immediately before handoff. A marketplace receipt should then be generated from an immutable settlement record, queued as an idempotent delivery intent, and retained as evidence with an explicit expiry. The least complex design is a transactional outbox plus a small audit record; it does not require preserving every rendered message forever.

TL;DR: retain the settlement identifiers, recipient decision, sender-authentication context, template version, payload digest, and delivery state for 90 days; keep the full rendered message only for the shorter interval that an actual dispute process needs. Treat a retry as the same receipt attempt, never as a new commercial event. This makes a later question answerable: what was owed, what was sent, under which domain, and what did the system know at the time?

The familiar welcome email deliverability checklist for a SaaS application still applies, but a post-settlement receipt has a stricter evidence boundary. A custom sending domain, DKIM, and a suppression list govern whether a message may leave the system; they do not replace a ledger-linked audit trail. The same rule holds if the application happens to use Node.js, even though the implementation below is Go: persistence and delivery must be separate concerns.

The decisive cost is usually retained message material, not the row that says a receipt was sent. For a planning model of 250,000 settled orders per day, 90 days produces 22.5 million receipts. At 8 KiB of headers plus rendered body per receipt, one raw copy is about 172 GiB; three independently retained copies are about 515 GiB before indexes, replication overhead, or attachments. Those are planning inputs, not a benchmark, but they force the useful question: which bytes will an investigator actually need?

## Start with the retention bill, then remove the wrong bytes

A receipt pipeline tends to accumulate copies in places that feel harmless in isolation: an outbox payload, a provider-facing message, an application log, a support export, and a backup. The operational bill is the product of volume, payload size, replication, and retention period. A compliance record needs enough context to reproduce the decision, while a mail archive needs the exact rendered content. Conflating the two gives every transient copy the longest retention period.

| Retained item | Planning size per receipt | 90-day use |
| --- | ---: | --- |
| Settlement and receipt identifiers | under 1 KiB | Reconciliation and idempotency |
| Recipient decision and template version | under 1 KiB | Explains why this recipient received this version |
| Rendered MIME body and headers | 8 KiB in this model | Resolves wording and header disputes |
| Payload digest and transition trail | under 1 KiB | Detects mutation without retaining the body |

The material change is to move the rendered body out of the durable outbox after successful handoff, leaving an immutable digest and a reference to a separately expiring archive. The queue can then store the minimum data required for retry, and the audit trail can remain compact. Keep the settlement record according to the organization's accounting and legal policy; the 90-day example is a message-evidence policy, not a universal retention rule.

Logs are archives.

If a support export or structured log contains the rendered body, shortening the primary archive does not shorten the actual retention surface. A data map should name those secondary stores before any deletion policy is called complete.

## Should a SaaS welcome email deliverability checklist treat custom domains as evidence?

The useful unit is a receipt evidence record tied to the settled order, rather than a claim that an email was "delivered." Mail transfer and mailbox placement are separate events, and a transport retry can occur after an uncertain acknowledgement. For payment systems, the defensible promise is narrower: one settled order creates one logical receipt obligation, and each attempted transmission is linked to that obligation. A custom domain is useful here because the record can state which sending identity was selected; it is not evidence that the recipient saw the message.

Record the order or ledger reference, settlement timestamp, currency and amount as represented by the accounting source, recipient address selected at the time, template revision, sending domain, authentication selector or key identifier, content digest, and every state transition. Each transition should carry a stable event ID and the time it was observed. The address is personal data in many regimes, so access to this record, export paths, and deletion workflows must be subject to the organization's applicable privacy and recordkeeping obligations.

The awkward case is a payment reversal that races with receipt dispatch. Do not overwrite the first evidence record. Append a reversal or correction event that points at the original receipt obligation, then issue a distinct correction message when policy requires one. The audit trail remains intelligible because it describes the sequence rather than rewriting it.

The trade-off is deliberate.

## Model sending as one logical obligation with many attempts

An outbox transaction makes the database commit that marks settlement visible to a worker without pretending that a database and an external mail system share a transaction. The worker may attempt delivery more than once. The business outcome remains exactly once because the receipt key is stable, and the append-only trail preserves attempts that could not be conclusively classified.

```go
package receipt

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

type ReceiptIntent struct {
	ID            string
	SettlementID  string
	Recipient     string
	TemplateRev   string
	SendingDomain string
	ContentDigest string
	CreatedAt     time.Time
}

func NewIntent(id, settlementID, recipient, templateRev, domain, body string, now time.Time) ReceiptIntent {
	digest := sha256.Sum256([]byte(body))
	return ReceiptIntent{
		ID: id, SettlementID: settlementID, Recipient: recipient,
		TemplateRev: templateRev, SendingDomain: domain,
		ContentDigest: hex.EncodeToString(digest[:]), CreatedAt: now,
	}
}
```

The unique constraint belongs on the logical key, such as `settlement_id` plus `receipt_kind`, at the same boundary that creates the outbox row. A worker lease is useful for throughput, but it is not an idempotency guarantee: a lease can expire while a request is in flight. The sending adapter should receive the intent ID as its idempotency token where the transport supports one; otherwise, the evidence model must be prepared to show an uncertain attempt and to apply a conservative retry policy.

```go
package receipt

import "context"

type Store interface {
	ClaimPending(ctx context.Context, limit int) ([]ReceiptIntent, error)
	AppendAttempt(ctx context.Context, intentID, outcome string) error
	MarkSent(ctx context.Context, intentID string) error
}

type Mailer interface {
	Send(ctx context.Context, intent ReceiptIntent, idempotencyKey string) error
}

func Dispatch(ctx context.Context, store Store, mailer Mailer, intent ReceiptIntent) error {
	if err := store.AppendAttempt(ctx, intent.ID, "started"); err != nil {
		return err
	}
	if err := mailer.Send(ctx, intent, intent.ID); err != nil {
		return err
	}
	return store.MarkSent(ctx, intent.ID)
}
```

Test the failure boundary explicitly. A test suite should simulate a crash after the sending adapter returns and before `MarkSent`, a duplicate settlement event, a recipient added to the suppression list after intent creation, and an expired archive reference. The last two catch a policy error: treating recipient eligibility as permanent, even though it can change before the worker sends.

This is where receipt systems fail.

## Authenticate the domain and enforce suppression at send time

A custom sending domain is evidence only when the system records which domain was selected and enforces that selection at the mail boundary. SPF is a DNS-published authorization mechanism. DKIM signs selected message fields so a receiver can evaluate whether the signed representation survived transit. Those mechanisms solve different problems, and neither turns a receipt into proof of inbox placement or welcome-email deliverability.

The dispatch path should validate that the visible sender domain is one the marketplace controls for this message class, that the current signing configuration is available, and that the recipient is eligible immediately before handoff. Suppression must be evaluated on every attempt, including retries. A hard suppression should end the obligation with a recorded policy reason, while a temporary condition should retain the intent for a bounded retry schedule. The distinction matters during reconciliation: "not sent by policy" is a completed decision, whereas "not yet sent" is outstanding work.

The evidence record should retain a selector or signing-key version, not the private key. It should also retain the policy version that classified a recipient as sendable or suppressed. Those two small fields are more useful in an audit than a large, unstructured application log.

Record the negative decision too.

## Expire content deliberately, and accept the investigation cost

At day 90, delete the rendered body and raw headers from the message archive after confirming that the compact evidence record remains readable and linked to the accounting record. Stop keeping template preview snapshots and raw transport responses beyond the approved operational window as well. This reduces the high-volume term in the retention model, but it has a real consequence: after expiry, an investigator can prove what content digest, template revision, recipient decision, and sending configuration existed; they cannot inspect the exact prose or every header of the original transmission.

That is an intentional trade-off, not a gap to hide. If a regulated complaint process requires the original rendered receipt for a longer period, extend that archive under a documented legal basis and protect it as a distinct records system. If it does not, retaining raw mail indefinitely creates a wider access and discovery surface without improving the settlement ledger.

Run periodic reconciliation across settled orders, outbox intents, terminal attempts, suppression decisions, and archive expirations. The report should identify missing obligations and contradictory terminal states, then preserve its own run timestamp and input range. A receipt workflow earns trust through those boring joins.

## Further reading: References

- https://datatracker.ietf.org/doc/html/rfc7208
- https://datatracker.ietf.org/doc/html/rfc6376
- https://datatracker.ietf.org/doc/html/rfc7489
- https://csrc.nist.gov/pubs/sp/800/92/final
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
