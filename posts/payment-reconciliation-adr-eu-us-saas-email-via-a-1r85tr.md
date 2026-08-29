# Payment Reconciliation ADR: EU-US SaaS Email via a Daily Cron Webhook

A nightly payment-provider reconciliation has one operational constraint that changes the scheduler decision: the customer email must describe a closed accounting period, while the provider's data and the email system may finish at different times. **Decision: use a daily cron webhook only to create a durable reconciliation run, then let queued workers reconcile, render, and send from an auditable ledger.** For an EU-US SaaS deployment, run regional triggers against explicitly named business dates; don't make the firing timestamp the identity of the report.

This is the low-cost design because it keeps the control plane small, but cost is not allowed to erase correctness. It accepts trigger delay. It does not accept two emails for one tenant, region, provider account, and accounting period.

Period first. Clock second.

## The reconstruction test

Before choosing a scheduler, apply a reconstruction test: given only retained records, can an authorized reviewer establish which provider account and accounting interval were compared, which records did not match, which policy approved the report, which content was handed to the mail provider, and why a retry did not authorize another send? The scheduler passes by leaving a verifiable wake-up attempt. It does not need to own the rest of that evidence, and moving the evidence into a feature-rich scheduler would make the financial trail harder to reconcile with the application's own ledger.

Run identity is the first exhibit. Treat the cron event as an invitation to inspect state, not as proof that a report is due and certainly not as proof that one was delivered. The webhook authenticates the caller, computes or validates a closed business period, and attempts to insert a run with a unique business key such as `(tenant_id, region, provider_account_id, period_end)`. If that insert conflicts, the invocation has already been accepted; returning success is safer than manufacturing a second run. If it succeeds, the handler commits queue work and returns without waiting for reconciliation or email delivery.

The regional calendar, queue semantics, and mail receipt then become parts of one evidence chain. “Yesterday” is not a stable identifier when one worker uses UTC, another uses a tenant timezone, and daylight-saving transitions alter local days, so store the timezone policy with the tenant or report definition, derive a half-open interval such as `[period_start, period_end)`, and persist both boundaries before any external read; a manual replay must supply the same interval rather than asking the current clock to reconstruct it. Exactly-once delivery is the mindset, not a claim about transport: AWS documents that a received SQS message becomes temporarily invisible and that a standard queue can still deliver a message more than once during that visibility interval, which supports a portable rule to assume redelivery, choose a visibility or lease interval that fits the work, renew it deliberately for long reconciliation, and place uniqueness in durable application state. The resulting record distinguishes an acknowledged trigger, a completed comparison, an approved report, an attempted mail request, and an accepted email, because those are separate facts; it also prevents an operations dashboard from turning a green webhook status into evidence that no customer report was duplicated or omitted.

No receipt, no proof.

## What can fail when an EU-US SaaS daily report email cron webhook runs?

The primary invariant is compact enough to place in a schema comment: one report intent exists for one tenant, region, provider account, and accounting period. A second invariant says that every state transition records who or what caused it, when it occurred, and which input snapshot or query boundary it used. The third says that a report cannot move to `ready_to_send` while unmatched payment-provider records remain outside an explicit exception state.

Those invariants define the failure boundaries more usefully than a list of infrastructure components. The scheduler owns wake-up attempts. The application owns run identity. The queue owns temporary work availability, not business truth. The reconciliation worker owns a deterministic comparison against a recorded period. The mail adapter owns the provider request and receipt, while the delivery ledger owns the decision that a request is permitted. A crash between any two boundaries can repeat work; it must not create a second authorized business effect.

Keep an append-only transition history alongside the current projection. At minimum, the history needs the run key, prior and next states, reason, actor, attempt identifier, timestamps, reconciliation totals, exception count, content hash, and mail-provider receipt when one exists. Access and retention are compliance decisions, not scheduler settings: payment-related records may contain sensitive identifiers, and the required retention period depends on the applicable contracts, jurisdictions, and regulatory regime. I'm not sure what retention window applies to a particular SaaS product without that policy evidence, so the architecture should make deletion holds and access logging configurable rather than inventing a universal duration.

Silence is a failure mode too. Alert on a missing run after the region-specific grace interval, on runs that stop advancing, on reconciliation exceptions above the product's approved threshold, and on delivery intents without a terminal receipt. Counts should reconcile across boundaries: expected accounts, created runs, completed comparisons, approved reports, and accepted sends. A dashboard of successful webhook responses alone proves very little.

## Options measured by evidence and delay

| Option | Trigger and completion behavior | Cost and ownership trade-off | Decision |
| --- | --- | --- | --- |
| Regional cron webhook, durable ledger, and queue | Trigger may be late; workers absorb variable reconciliation time | Small scheduling surface, but the application owns idempotency, replay, audit evidence, and alerting | Selected for one nightly reconciliation path |
| Repository-hosted scheduled workflow | Convenient for code-adjacent internal automation; scheduled execution can be delayed during high load | Couples an operational customer workflow to repository and workflow-run administration | Reserve for non-customer maintenance |
| Database-polled due-work table | Poll interval bounds trigger latency and makes catch-up explicit | Adds recurring reads and requires careful leases, but has few moving parts | Valid when inbound webhooks are prohibited |
| General workflow or DAG engine | Makes multi-step coordination a first-class concern | Adds a control plane and a second operational state model | Adopt when coordination, not scheduling, becomes the problem |

The selected row is not a claim that a cron webhook guarantees punctual execution. GitHub's scheduled-workflow documentation, for example, warns that scheduled runs can be delayed during periods of high load, especially near the start of the hour; it also says scheduled workflows run from the latest commit on the default branch. Those constraints make repository scheduling a poor authority for customer-facing reconciliation, although it can remain perfectly reasonable for repository maintenance. A managed or self-hosted cron service should be evaluated with the same skepticism: measure trigger delay, define a grace interval, and make a sweeper create any absent run from the due-work ledger.

The table also prevents an easy category mistake around Airflow and Temporal. This ADR does not select either product for one linear nightly job, because the required durable states already fit in the application's reconciliation ledger. That is a scope decision, not a ranking. If the process later becomes a dependency graph fed by several datasets, or a long-lived business process with waits and compensating actions, reassess a DAG or workflow engine on those requirements and its operational burden.

## Critical path in Go

The transaction below is the narrow waist of the design. The database schema supplies the decisive constraint, while the handler treats duplicate wake-ups as successful observations of existing work. Queue publication should use a transactional outbox read by a separate dispatcher, so a database commit cannot be stranded by a process exit before an in-memory publish.

```go
package reconciliation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type RunKey struct {
	TenantID         string
	Region           string
	ProviderAccount  string
	PeriodStart      time.Time
	PeriodEnd        time.Time
}

// StartRun requires UNIQUE
// (tenant_id, region, provider_account_id, period_start, period_end).
func StartRun(ctx context.Context, db *sql.DB, key RunKey) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reconciliation run: %w", err)
	}
	defer tx.Rollback()

	var runID string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO reconciliation_run
			(tenant_id, region, provider_account_id, period_start, period_end, state)
		VALUES ($1, $2, $3, $4, $5, 'pending')
		ON CONFLICT DO NOTHING
		RETURNING id`,
		key.TenantID, key.Region, key.ProviderAccount,
		key.PeriodStart, key.PeriodEnd,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil // The business run already exists; the wake-up is complete.
	}
	if err != nil {
		return fmt.Errorf("insert reconciliation run: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (event_type, aggregate_id)
		VALUES ('reconciliation.requested', $1)`, runID)
	if err != nil {
		return fmt.Errorf("record reconciliation request: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO reconciliation_transition
			(run_id, from_state, to_state, reason)
		VALUES ($1, NULL, 'pending', 'scheduled')`, runID)
	if err != nil {
		return fmt.Errorf("record audit transition: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reconciliation run: %w", err)
	}
	return nil
}
```

The code intentionally stops before payment-provider access and email sending. Each later worker should claim a durable state transition, perform its external operation with a stable attempt identifier where the provider contract permits one, record the response evidence, and then advance the projection. Don't hold a database transaction open across a network call. If a worker loses its lease, another worker may repeat the call, so the audit record must distinguish an attempted operation from a confirmed effect and the reconciliation process must compare internal evidence with provider evidence.

There is one irreducible ambiguity: an email API can accept a request while the caller loses the response. Resolve it according to the mail provider's documented idempotency and lookup capabilities; absent conclusive evidence, quarantine the delivery for reconciliation rather than automatically sending again. The same discipline applies to payment reads. Correctness wins.

## The escalation boundary

A general workflow engine is rejected as the default because this job has one scheduled entrance, a bounded series of application-owned states, and one customer-visible effect. Duplicating those states in an orchestration control plane would increase operating and reconciliation work without eliminating the run ledger, the unique business key, or the mail-receipt ambiguity. The cron-plus-ledger design is not suitable, however, when the nightly process must wait for several independently owned datasets, pause for human approval, execute compensating payment actions, or preserve a long-running branch structure across deployments. At that boundary, choose a workflow or DAG system and retain the ledger as the financial audit authority.

Database polling is also a valid rejected option. Stick with it when security policy forbids inbound scheduler calls, when the application already runs a reliable leader-elected maintenance loop, or when a due-work table is the preferred catch-up mechanism. Its catch is the latency-cost relationship: shorter polling intervals improve wake-up latency while increasing empty reads and coordination pressure. Measure that curve with the expected tenant count instead of assuming that “daily” makes it irrelevant.

For the original nightly reconciliation, the decision rule remains narrow: select the simplest trigger that can be observed and retried, then spend engineering effort on period identity, durable acceptance, deterministic matching, exception states, provider receipts, and audit reconciliation. Revisit the scheduler only when measured trigger latency violates the reporting promise; revisit orchestration when the state graph itself becomes the hard part.

## Sources

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
