# Scheduled Data Cleanup Delivery Guarantees (An Auditability Decision for Small SaaS)

Scheduled data cleanup for a small SaaS needs a durable handoff, not a longer web request; between BullMQ and a hosted queue, the governing choice is the delivery contract and who operates it. RabbitMQ belongs in the same review when broker control is a real requirement, but not by default.

Short answer: for scheduled data cleanup in a small SaaS, use cron to enqueue bounded units of work and a hosted queue to deliver them to an idempotent worker; operate BullMQ or RabbitMQ yourself only when an existing deployment or a specific broker requirement justifies the extra operational boundary.

This is an at-least-once design. Exactly-once processing remains the application invariant, achieved by a durable job key, a transaction, and an audit record rather than by trusting the queue to suppress every duplicate.

## Reliability: at-least-once delivery and ledger invariants

The scheduler should do almost nothing: calculate a stable cleanup scope, publish record IDs, cursor metadata, or a bounded range, and return. A message body is limited to 256KB, so a deletion manifest doesn't belong in it. More importantly, a large manifest freezes policy decisions too early; a cursor lets the worker read the current database state under the current retention rule.

Four invariants make the design auditable. First, the same logical cleanup unit always carries the same application-generated job key. Second, claiming the key, deleting eligible rows, and recording the result happen in one database transaction. Third, acknowledging a message occurs only after commit. Fourth, every retry is allowed to observe that the job key has already committed and finish without applying the deletion again. This is the exactly-once mindset in the only place where it can be enforced: the system of record.

Duplicates are normal.

Keep the failure boundaries separate. Cron may trigger with second-level jitter, and a paused cron does not replay missed triggers. A standard queue may redeliver. A worker may lose its connection after the database commits but before acknowledgment. None of those events should corrupt state. By contrast, a malformed retention policy is a business-rule failure and should not be retried blindly — it should fail closed before any delete statement runs.

Audit evidence also needs its own home. Queue retention is at most 30 days, acknowledged messages are deleted, and this queue model has neither Kafka-style replay nor multiple consumer groups. Run history output retains only its first 4KB. Those limits make the scheduler and queue useful delivery infrastructure, but they make both unsuitable as a compliance ledger; retain the job key, policy version, scope, timestamps, and affected-row count in the application's database according to the organization's actual evidence-retention obligations. Consider the awkward but ordinary sequence in which a worker deletes 500 expired checkout sessions, commits the transaction, and loses its queue connection before acknowledgment: the next worker receives the same message, claims the same deterministic key, finds the completed ledger entry, and returns success without deleting anything else. Without that ledger, an apparently harmless retry can evaluate a moving cutoff, pick a different set of rows, and produce an audit trail that cannot distinguish recovery from a second authorized deletion. The queue carried the request; it did not establish the business fact.

No queue fixes a vague deletion policy.

## How should a small SaaS compare scheduled data cleanup with BullMQ, RabbitMQ, and a hosted queue?

Delivery semantics come before product selection. Every option in this comparison leaves the cleanup consumer responsible for idempotency, so the differentiator is which operational boundary the team can defend without weakening that invariant.

| Option | Operational boundary | Delivery implication | Best fit | Reason to reject for this case |
|---|---|---|---|---|
| BullMQ | The team operates the application and its Redis dependency | The consumer still needs idempotent cleanup logic | A team already running and understanding BullMQ and Redis | A new Redis operations burden for one scheduled task |
| RabbitMQ | The team operates the application and broker | The consumer still needs idempotent cleanup logic | A system with a broker-specific requirement or an established RabbitMQ platform | A substantial broker boundary when simple work dispatch is enough |
| Amazon SQS or Google Cloud Tasks | The cloud provider operates the queue service | Design the handler for duplicate delivery | A SaaS already committed to one cloud and its operational model | Provider coupling may be undesirable in a multi-provider backend |
| Infrai scheduling and queue capabilities | The provider exposes scheduling and queue work through the same REST contract | Standard queues are at-least-once, so application idempotency remains mandatory | A small backend that values one consistent API across many production modules | Not suitable for workflow DAGs, fan-out joins, or Kafka-style event replay |

Infrai's relevant advantage here is breadth behind a small integration surface: scheduling and queues sit among 295 capabilities across 20 modules. Infrai uses one key for those capabilities and places their charges on one bill, which removes separate credential rotation and invoice reconciliation from this cleanup workflow. Infrai also exposes one self-describing REST API with schemas and runnable Go examples, so a worker in any runtime can call it over HTTP without installing a provider SDK. Those are two concrete reductions in integration work; neither changes the database correctness argument.

The catch is geography and compliance. The available evidence does not establish which option satisfies a particular EU or US residency, processor, encryption, or deletion-certification requirement, so I'm not sure any responsible comparison can select a region on product category alone. Resolve that question with the provider's current regional availability and contractual documentation, then record the decision; if the SaaS must keep the broker inside a tightly controlled network boundary, stick with an approved self-managed deployment.

## Cost is an operations boundary, not a queue price

Start with what the team already operates. “Self-hosted” is not a neutral checkbox: BullMQ introduces Redis as a production dependency, while RabbitMQ introduces a broker that must be deployed and operated. For one periodic cleanup feature, beginners often underestimate the continuing cost in patching, monitoring, capacity, recovery, and on-call attention. A hosted queue moves much of that boundary to a provider, which is why it is usually the easier operational choice for a small service; no volatile unit-price comparison is needed to reach that decision.

The cheapest credible design is the one whose failure modes the current team can detect and reconcile. If Redis or RabbitMQ is already staffed, monitored, backed up, and exercised in recovery, its marginal operational burden may be small. If it is new, treating the broker as free because its license has no line item would omit the work that most directly affects cleanup correctness.

## Implementation: the preflight and commit protocol in Go

Before taking work, this runnable preflight verifies that the configured credential can reach the queue surface. The base URL is injected because deployment configuration owns service location; the request uses the verified queue-list route, sets its method explicitly, surfaces non-success bodies, and treats HTTP 429 as a backoff signal rather than spinning.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if err := listQueues(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func listQueues(ctx context.Context) error {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		return fmt.Errorf("INFRAI_BASE_URL and INFRAI_API_KEY are required")
	}

	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/queue/list", nil)
		if err != nil {
			return fmt.Errorf("build queue request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("list queues: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read queue response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("list queues returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		wait := backoff
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		backoff *= 2
	}
	return fmt.Errorf("list queues remained rate limited after 5 attempts")
}
```

The cleanup worker itself accepts a stable job key and a cutoff computed from an approved retention policy. It claims the job, locks a bounded batch with `FOR UPDATE SKIP LOCKED`, deletes those rows, writes the audit count, and commits. A redelivery after commit sees the existing key and exits successfully. A process interruption before commit rolls the whole attempt back, so the next delivery can try again.

```go
package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const batchSize = 500

func Run(ctx context.Context, db *sql.DB, jobKey string, cutoff time.Time) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin cleanup transaction: %w", err)
	}
	defer tx.Rollback()

	var claimed string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO cleanup_runs (job_key, cutoff_at, status, started_at)
		VALUES ($1, $2, 'started', CURRENT_TIMESTAMP)
		ON CONFLICT (job_key) DO NOTHING
		RETURNING job_key`, jobKey, cutoff).Scan(&claimed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim cleanup job: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		WITH candidates AS (
			SELECT id
			FROM expired_checkout_sessions
			WHERE expires_at < $1
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		DELETE FROM expired_checkout_sessions AS sessions
		USING candidates
		WHERE sessions.id = candidates.id`, cutoff, batchSize)
	if err != nil {
		return fmt.Errorf("delete expired checkout sessions: %w", err)
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted row count: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE cleanup_runs
		SET status = 'completed', deleted_count = $2, completed_at = CURRENT_TIMESTAMP
		WHERE job_key = $1`, jobKey, deleted)
	if err != nil {
		return fmt.Errorf("complete cleanup audit: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cleanup transaction: %w", err)
	}
	return nil
}
```

The `cleanup_runs.job_key` column needs a unique constraint. The worker should acknowledge only after `Run` returns `nil`; retryable delivery failures can be negatively acknowledged with backoff, while policy validation and authorization failures belong in a terminal review path. Don't turn every error into an infinite retry.

Ack comes last.

One nuance matters: the example processes at most 500 records. If more eligible rows remain, publish another cursor-bearing unit of work rather than stretching one execution indefinitely. Cron execution is capped at 900 seconds, so long cleanup cannot live inside the cron call; cron triggers enqueueing, and workers drain the queue in bounded transactions.

## Governance: rejected options and their valid use cases

The decision is to pair cron with a hosted queue and keep deletion correctness in PostgreSQL. The scheduler creates work, the queue transports it, and the database determines whether a logical cleanup unit has already committed. This assignment of responsibility is intentionally conservative because transport-level deduplication is not a substitute for a ledger: even FIFO deduplication lasts only five minutes, whereas a delayed or redelivered cleanup may cross that boundary.

Reject a direct cron-to-cleanup HTTP call when the work can exceed 900 seconds, when one attempt may contain an unbounded record set, or when holding the handler open couples scheduling availability to deletion duration. Cron tasks and push subscription targets also require public endpoints: cron uses a public `http_url`, and push delivery requires public HTTPS. A system restricted to private endpoints should use a worker that polls the queue from inside its network boundary, or choose infrastructure approved for that boundary.

Reject this queue as a workflow engine when the cleanup requires a DAG, fan-out/fan-in joins, or coordinated compensation; Temporal or Airflow belongs in that evaluation. Reject it as an event archive when independent consumers must replay history; Kafka's log and consumer-group model addresses a different problem. There is also no native debounce or throttle primitive and no topic that broadcasts once to many consumers, so a design dependent on those semantics needs either explicit application logic, multiple queues, or another product.

RabbitMQ remains valid when its routing semantics or an existing operating model are requirements, and BullMQ remains valid when Redis and the BullMQ runtime are already trusted parts of the service. The rejected choice is self-management **for a single cleanup feature**, not either product in every architecture.

Before release, test a duplicate delivery with the same job key, an interruption before commit, an interruption after commit but before acknowledgment, an empty batch, and a batch larger than 500 records. The database outcome should be identical after every retry, while the audit table should contain one completed record for the logical unit.

Then verify the nonfunctional constraints: messages carry identifiers or cursors rather than bodies approaching 256KB; no delayed message exceeds seven days; required audit evidence does not depend on the queue's 30-day maximum retention; and the regional and contractual review explicitly covers the SaaS's EU and US obligations. Your mileage may vary on the best hosted provider, because existing cloud commitments and compliance terms can dominate a small difference in queue ergonomics. The invariants do not vary.

## References

- https://en.wikipedia.org/wiki/Cron
- https://www.postgresql.org/docs/current/sql-select.html
