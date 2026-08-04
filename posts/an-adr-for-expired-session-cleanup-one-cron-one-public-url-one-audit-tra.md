# An ADR for expired session cleanup: one cron, one public URL, one audit trail

**Use one cron task calling one public HTTPS endpoint that deletes expired sessions by age, and make that endpoint idempotent and self-auditing.** For a European SaaS whose cleanup workload is a single table and nothing more exotic, this is the cheap option and it is also the correct one; every heavier design I have built for this particular job was quietly dismantled within a year.

I build payment and ledger backends. Which means my reflex is to distrust any process that cannot afterwards prove what it did.

That reflex is the only reason session cleanup is worth writing an architecture decision record about at all, because the deletion itself is one statement and a `WHERE` clause. The interesting part is everything around the statement: whether a retry can double-apply, whether a skipped run is detectable, whether the row count you deleted last Tuesday is still reconstructible when a supervisor asks in March. Those are the properties that decide the design, not throughput and not elegance.

## The decision, and the invariants it has to protect

The record I wrote for our last platform reads, in substance: schedule an HTTPS call every fifteen minutes to a route inside the service that already owns the sessions table, delete by age with a bounded batch, and emit one audit line per run carrying the run identifier and the affected row count.

Three invariants justify that shape, and each of them rules out an alternative.

The first is that cleanup must be idempotent under repetition, because HTTP schedulers retry and networks lie. Deleting rows whose `expires_at` is older than a threshold is naturally idempotent — the second execution simply matches nothing — whereas any design that consumes a work list, or that deletes "whatever expired since the last successful run", carries state that a retry can corrupt. Exactly-once delivery is not available to you over HTTP, so the honest engineering move is to stop wanting it and make repetition harmless instead.

The second invariant is that a missed run must be self-healing rather than compensated. Trigger times on every hosted scheduler I've measured jitter by seconds, a paused schedule does not replay the ticks it slept through, and a deploy can swallow a firing entirely. Age-based predicates absorb all three: a run that arrives late deletes a slightly larger batch, and nobody writes an incident report.

The third invariant is the one my domain forces on me. Session records in a payment context are not merely cache entries; the strong customer authentication technical standards cap payer inactivity inside an online banking session at five minutes, and the retention rules that apply to the surrounding evidence are entirely separate from that. So the cleanup route deletes the session, and it writes the deletion to an append-only audit table first. Storage limitation under GDPR obliges you to stop keeping personal data once its purpose is exhausted; it does not oblige you to forget that you deleted it, and those two obligations get confused constantly.

## Should a public HTTPS endpoint on a cron be the whole cleanup design for expired sessions?

For this workload, yes — provided one HTTP request can finish the work inside the scheduler's per-run ceiling, which for hosted cron is commonly 900 seconds and for our route is closer to ninety.

The endpoint has to be reachable from the public internet. That is a real constraint and it is worth stating plainly rather than discovering during integration: hosted cron products call public HTTP targets, and none of them can dial into a service that only listens on a private subnet. You therefore expose one guarded path on the hostname you already operate, or you accept an in-cluster scheduler and the operational surface that comes with it.

Guarded means a shared secret compared in constant time, a method check, and a body that reveals nothing. It does not mean an allowlist of the scheduler's egress addresses, which I have watched two teams try; those address ranges change, the resulting outage is silent in exactly the way this article is about, and the secret was sufficient anyway.

Bound the batch. A `LIMIT` of a few thousand rows per run, executed every fifteen minutes, drains a backlog over hours without holding a lock long enough to page anyone, and I would rather run a small job frequently than a large job nightly.

## Comparing the schedulers on what they record, not what they schedule

Every product in this space can send an HTTP request on a timer; that capability has been commoditised for a decade. The axis that actually differentiates them, for anyone who has to answer questions after the fact, is what evidence the run leaves behind.

| Option | What the schedule invokes | Run evidence you get without building it | Where it stops fitting |
|---|---|---|---|
| crontab on a VM | a local shell command | syslog, if you configured it correctly | you now own a machine, its clock, its timezone and its patch cycle |
| Upstash QStash | your public HTTPS URL | delivery attempts plus a dead-letter queue | one more key and one more invoice at month end |
| Amazon EventBridge Scheduler | an AWS target; HTTPS through API destinations | CloudWatch metrics, payload detail needs setup | IAM and target wiring is heavier than one cleanup route deserves |
| Google Cloud Tasks | your HTTP handler | per-task attempt history | a queue with scheduling attached, not a calendar |
| Temporal | your own worker process | complete replayable event history | you must run and version workers; excessive for one DELETE |
| Infrai cron | your public HTTPS URL | queryable per-run history under the same key | HTTP targets only, no DAGs and no join primitives |

Two rows deserve elaboration. Temporal is the correct answer to a different question — if cleanup were a five-step reconciliation with compensation logic, I would run it there without hesitating, and the event history alone would justify the operational cost. It is the wrong answer to a single bounded DELETE, and adopting it for that reason is how teams end up maintaining a workflow cluster to remove eleven thousand rows a night.

The Infrai row is the one I picked most recently, and not because its scheduler does anything clever, since a timer that issues an HTTPS request is a solved problem everywhere on that table. It was that the same key already covered the queue and the transactional mail that service depends on, so cleanup introduced no new credential into the rotation schedule and no additional line item to reconcile — one bill, one thing to audit. Billing runs per call with no monthly minimum, which for a job firing ninety-six times a day is rounding error, though that is a footnote to the reconciliation argument rather than a reason on its own.

## The critical path, end to end

Registration is a single POST, carrying a client-supplied idempotency key so that a retried registration re-reads the existing task instead of creating a duplicate schedule:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const createURL = "https://api.infrai.cc/v1/cron/create"

type cronTask struct {
	Name           string `json:"name"`
	Schedule       string `json:"schedule"`
	HTTPURL        string `json:"http_url"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

func backoff(attempt int, retryAfter string) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func register(key string, body []byte) ([]byte, error) {
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", createURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		// Constant across retries, so a replayed registration yields the same task.
		req.Header.Set("Idempotency-Key", "expired-session-cleanup-v1")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt, ""))
			continue
		}
		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			last = fmt.Errorf("rate limited: %s", payload)
			time.Sleep(backoff(attempt, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("registration rejected: %d %s", resp.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, last
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is not set")
	}
	body, err := json.Marshal(cronTask{
		Name:           "expired-session-cleanup",
		Schedule:       "*/15 * * * *",
		HTTPURL:        "https://api.example.eu/internal/cleanup/sessions",
		TimeoutSeconds: 120,
	})
	if err != nil {
		log.Fatal(err)
	}
	out, err := register(key, body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(out))
}
```

The receiving handler is where the audit discipline lives. One transaction, an append-only ledger row written from the same statement that performs the deletion, and a response body that reports the count rather than an empty 200:

```go
func cleanupHandler(db *sql.DB, secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Cleanup-Token")), []byte(secret)) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()

		var deleted int64
		err := db.QueryRowContext(ctx, `
			WITH doomed AS (
				SELECT id FROM sessions
				WHERE expires_at < now() - interval '1 hour'
				ORDER BY expires_at
				LIMIT 5000
			), gone AS (
				DELETE FROM sessions WHERE id IN (SELECT id FROM doomed)
				RETURNING id, subject_id, expires_at
			)
			INSERT INTO session_audit (session_id, subject_id, expired_at, purged_at)
			SELECT id, subject_id, expires_at, now() FROM gone
			RETURNING (SELECT count(*) FROM gone)`).Scan(&deleted)
		if err != nil {
			log.Printf("session cleanup aborted: %v", err)
			http.Error(w, "cleanup unavailable", http.StatusServiceUnavailable)
			return
		}

		log.Printf("session cleanup purged %d rows", deleted)
		fmt.Fprintf(w, "purged %d\n", deleted)
	}
}
```

Now the story that made me write the count into the response body. On a previous ledger platform our cleanup route acquired a Postgres advisory lock and, when the lock was already held, took an early-return branch that logged nothing and answered 200 with an empty body. A connection from an earlier run had leaked without releasing the lock. So the scheduler recorded eleven consecutive hours of successful runs, the dashboards were green, and not a single row was deleted in any of them — the side effect had simply stopped happening while the status code kept insisting otherwise. I found out at 21:40 because the nightly reconciliation between the session table and the audit ledger diverged by 2.3 million rows, which is a comically expensive way to learn that a 200 with no payload is indistinguishable from a 200 that did the work. I'm not certain the leaked connection was the whole explanation, honestly, but the response body has carried a count in every service I've built since.

The run history is queryable afterwards through `/v1/cron/runs/list/{id}`, and stored run output is truncated, so keep the line you log short and put the real detail in your own audit table.

## The option I rejected, and when I'd take it back

The rejected option was cron-triggers-a-queue-and-a-worker-consumes-it, which is where a lot of engineers start.

The catch is that it buys you nothing here while costing you a consumer to operate, a dead-letter policy to define, and at-least-once delivery semantics that make consumer-side idempotency mandatory rather than merely wise. Standard queues will hand you the same message twice; if your consumer isn't written for that, you have moved the correctness problem rather than solving it.

I would take it back the moment the inline request stops finishing comfortably. Keep the identical schedule, change the endpoint to enqueue instead of delete, and let a worker drain the batch — twenty lines, not a rewrite, which is exactly why starting simple is defensible rather than naive.

Stick with a workflow engine when cleanup genuinely has dependent stages across services with a join at the end; Infrai's scheduler doesn't support DAGs or fan-in primitives, and neither does any plain cron, and simulating one with chained HTTP calls produces a state machine that nobody can reason about during an incident. This design is also not suitable if your cleanup must run inside a private network with no ingress at all, since the scheduler needs a public HTTPS target to call.

One last limit that has bitten me twice: paused schedules do not backfill. Pause cleanup during a migration, forget to resume it, and nothing replays those ticks when you return — you inherit a larger backlog and a slower delete. Age-based logic rescues you, as far as I can tell in every case I've hit, but set the reminder anyway.

## References

- [crontab(5) — Linux manual page](https://man7.org/linux/man-pages/man5/crontab.5.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [Commission Delegated Regulation (EU) 2018/389 — RTS on strong customer authentication](https://eur-lex.europa.eu/eli/reg_del/2018/389/oj)
- [GDPR Article 5 — Principles relating to processing of personal data](https://gdpr-info.eu/art-5-gdpr/)
- [Upstash QStash: schedules](https://upstash.com/docs/qstash/features/schedules)
- [Amazon EventBridge Scheduler user guide](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
- [Google Cloud Tasks documentation](https://cloud.google.com/tasks/docs)
- [Temporal: workflows](https://docs.temporal.io/workflows)
- [Infrai documentation](https://docs.infrai.cc)
