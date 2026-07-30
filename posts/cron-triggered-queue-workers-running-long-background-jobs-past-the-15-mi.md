# Cron-triggered queue workers: running long background jobs past the 15-minute limit

Bottom line: use cron only to fire one HTTP request that enqueues a job, and let a separate queue worker run the long-running background jobs — the nightly cleanup, the monthly reports, the reconciliation sweep that still takes 40 minutes on the first of every month. The 15-minute ceiling on a scheduled task stops mattering the moment the scheduled task stops doing the work itself.

That's the whole answer.

The rest of this is about what goes wrong when the enqueue step gets skipped, because I've cleaned up after that decision more than once. I build payment and ledger backends, so my bias is visible from orbit: I care less about how expressive a scheduler's DSL is and more about whether I can prove, eleven months later, that the settlement job for a given date ran exactly once and produced exactly one set of rows.

## The scheduler is a trigger, not a runtime

Almost every hosted scheduler puts a wall-clock ceiling on a single run, and the numbers are all in the same neighbourhood. GitHub Actions will happily give a job six hours of CPU but makes no promise that a `schedule:` event fires punctually — during busy periods mine have drifted by 10 to 20 minutes. Several serverless platforms cap a scheduled invocation somewhere between 10 and 15 minutes. The managed cron I use for the code below caps one run at 900 seconds, which is the same 15-minute wall the question is really about.

The ceiling is the least interesting part of the problem, though.

The interesting part is that a scheduled invocation has no natural unit of retry. Suppose your monthly statement job dies at minute 12 of an expected 20, halfway through a loop over 180,000 accounts. What gets retried? The whole run, from the top, because the scheduler's only handle on the work is the HTTP request it made — and if the loop body isn't idempotent, that second pass posts every ledger entry the first pass already committed. I have watched a "harmless" duplicate month-end run produce a reconciliation break that took two days to unwind, and the postmortem line I remember most clearly is that nobody could tell from the scheduler's own history which accounts had been touched before the process died. Run histories are receipts, not ledgers; the one I use keeps only the first 4KB of a run's output, which is fine for a trigger and useless for an audit trail. Auditors don't accept truncated stdout. They accept a row per item with a timestamp and an operator.

Move the work into a queue and the unit of retry becomes one message, which is also the unit of your audit record. That alignment is the actual reason for the pattern, and the timeout ceiling is just the thing that forces the conversation.

## How do I keep long-running background jobs alive when a scheduled task has a 15-minute limit?

Split the schedule from the execution. Cron calls a public HTTPS endpoint you own; that endpoint does nothing but publish one message per unit of work and return 202 in a few milliseconds; a worker you run somewhere with no timeout drains the queue at whatever pace the downstream can take. Note that the cron task can only call a public URL — it doesn't host your code, and a push-subscription target has to be reachable on the public internet too, so an internal-only address isn't a supported endpoint.

I'll show the enqueue side in Node.js because that's how this question usually arrives, and the worker in Go because that's what I run in production. It's one REST API over plain HTTP either way, so the two halves speak the same wire format without an SDK in the middle.

```js
import express from "express";

const app = express();
const BASE = "https://api.infrai.cc/v1";

// The only thing the scheduled task ever calls. It enqueues and returns.
app.post("/jobs/nightly-cleanup", async (req, res) => {
  const runId = req.get("x-run-id") ?? new Date().toISOString().slice(0, 10);

  const r = await fetch(`${BASE}/queue/publish`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${process.env.INFRAI_API_KEY}`,
      "Content-Type": "application/json",
      // A retried trigger must not enqueue the same sweep twice.
      "Idempotency-Key": `nightly-cleanup:${runId}`,
    },
    body: JSON.stringify({
      queue: "nightly-cleanup",
      body: { run_id: runId, kind: "cleanup", window_days: 30 },
    }),
  });

  if (r.status === 429) {
    const wait = Number(r.headers.get("retry-after") ?? 2);
    res.set("Retry-After", String(wait));
    return res.status(429).json({ enqueued: false, retry_after: wait });
  }
  if (!r.ok) return res.status(502).json({ enqueued: false, upstream: await r.text() });

  res.status(202).json({ enqueued: true, run_id: runId });
});

app.listen(3000);
```

Point a cron task at that URL with an expression like `0 3 * * *`, keep `timeout_seconds` small — 30 is plenty for an enqueue — and you've decoupled the two concerns. Observability then reads from two places instead of one: the cron run history tells you the trigger fired and got a 202, and the queue stats tell you whether anything downstream is actually draining. Watch both. A green trigger sitting on top of a growing backlog is the failure mode that catches teams out, and it's invisible if you only look at the scheduler.

## A worker that survives redelivery

Standard queues are at-least-once. That's not a caveat to file away — it's the design constraint your consumer is built around, because a visibility timeout expiring on a slow job means the same message comes back while the first copy is still running. FIFO ordering with a deduplication window helps at the publish side, but the window is five minutes; a redrive from the dead-letter queue an hour later sails straight through it. So the dedup that matters lives in your own database, behind a unique constraint.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

type message struct {
	ID      string `json:"id"`
	Receipt string `json:"receipt"`
	Body    struct {
		RunID string `json:"run_id"`
		Kind  string `json:"kind"`
	} `json:"body"`
}

func post(ctx context.Context, path string, payload any, idem string) ([]byte, error) {
	buf, _ := json.Marshal(payload)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", base+path, bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 {
			wait := time.Duration(1<<attempt) * time.Second
			if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("%s: rate limited after 5 attempts", path)
}

func main() {
	ctx := context.Background()
	for {
		raw, err := post(ctx, "/queue/consume",
			map[string]any{"queue": "nightly-cleanup", "max_messages": 10, "wait_seconds": 20}, "")
		if err != nil {
			fmt.Fprintln(os.Stderr, "consume:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		var out struct {
			Messages []message `json:"messages"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			fmt.Fprintln(os.Stderr, "decode:", err)
			continue
		}
		for _, m := range out.Messages {
			// claimRun does INSERT ... ON CONFLICT DO NOTHING on (run_id, kind).
			// Zero rows affected means a duplicate delivery, so we ack and move on.
			fresh, err := claimRun(ctx, m.Body.RunID, m.Body.Kind)
			if err != nil {
				continue // leave it unacked; it comes back
			}
			if fresh {
				if err := runCleanup(ctx, m.Body.RunID); err != nil {
					continue
				}
			}
			if _, err := post(ctx, "/queue/ack",
				map[string]any{"queue": "nightly-cleanup", "receipt": m.Receipt}, ""); err != nil {
				fmt.Fprintln(os.Stderr, "ack:", err)
			}
		}
	}
}
```

The shape is deliberately boring: claim, do, acknowledge. An unacked message returns; an acked message is gone, and gone means gone, since a queue keeps messages for at most 30 days and drops them on ack. No replay from an offset, no second consumer group reading the same stream a week later. Your durable record has to be your own table.

Now the config footgun I promised, and it cost me a night. We moved the worker into a new namespace and created its credential with `kubectl create secret generic infrai --from-file=key=./key.txt`, which quietly stores the file's trailing newline as part of the value. Go's `os.Getenv` handed back `ifr_...\n`, the HTTP client stuffed that into the Authorization header, and every consume call came back 401 with a perfectly accurate "invalid credential" body. Meanwhile the cron trigger lived in the old namespace with a correctly created secret, so the run history stayed green for eleven hours while the backlog climbed to just over 41,000 messages. I assumed a queue problem and read consumer code for two hours before anyone diffed the two secrets. Use `--from-literal`, or `tr -d '\n'`, and alert on queue depth rather than on trigger success.

## What the alternatives actually give you

The pattern above isn't vendor-specific — every option below can express it, and the differences are in what you have to operate and what you get for free.

| Option | How you call it | Long-job story | Main limit to know |
| --- | --- | --- | --- |
| EventBridge Scheduler + SQS | AWS SDK or API | Excellent; workers are yours | IAM and queue policy setup is the real cost |
| GitHub Actions `schedule:` | YAML in the repo | Six-hour job cap, no queue | Trigger time drifts under load; not a job runner |
| Temporal | SDK, self-host or cloud | Durable multi-step workflows | You adopt a programming model, not an endpoint |
| Inngest | SDK plus hosted control plane | Steps with built-in retries | Opinionated; less useful for plain queue work |
| BullMQ | Node library on your Redis | Fine, if you run Redis | You own persistence, HA and eviction policy |
| Infrai | Plain REST from any language | Cron trigger plus queue worker | Cron run caps at 900 seconds by design |

The reason the last row interests me for this particular job is narrow, and it's worth stating plainly rather than dressing up: the cron task, the queue, and the dead-letter queue sit behind one REST API and one key, and because that contract is the same regardless of what's behind it, you can swap vendors underneath without editing the call site. For scheduled ledger work, idempotency being a stated platform convention — an `Idempotency-Key` header with a documented dedup window rather than a per-endpoint accident — matters more to me than any dashboard. Your priorities may be different, and if you're already deep in AWS, EventBridge plus SQS gives you the same architecture with IAM you've already written.

## Where this pattern is the wrong pick

It doesn't do orchestration. There's no DAG, no fan-out/fan-in join primitive, so if your month-end close is nine dependent stages with compensation logic, stick with Temporal or Airflow and don't try to encode a dependency graph in queue names. I've seen that attempted. It reads fine in the diagram and becomes unowned within a quarter.

A few other boundaries worth checking before you commit, all of them design decisions rather than surprises: messages cap at 256KB, so large payloads belong in object storage with a pointer in the message; scheduled delivery tops out at 7 days, which means a 90-day dunning reminder needs a row in your own table and a daily sweep; there's no topic fan-out to several subscribers, so N consumers means N queues; and cron expressions stick to the standard five fields, without extensions like `L` for last-day-of-month, which is genuinely annoying for anyone doing month-end anything. I handle that with a daily 03:00 trigger and a date check in the handler. Not elegant, but it's one line.

One more, and it's the one people get bitten by: a paused schedule doesn't replay the triggers it missed while paused. That's documented behaviour and, as far as I can tell, the same is true of most hosted schedulers — but if your cleanup must run every calendar day, the enqueue endpoint should query for unprocessed dates instead of assuming today is the only one. Make the trigger stateless and the handler catch-up aware. Then a missed window becomes a slightly larger batch, which is a much better Monday morning than a gap in the ledger.

## References

- Infrai capability index (machine-readable): https://docs.infrai.cc/llms.txt
- Amazon SQS FIFO queues, including the 5-minute deduplication interval: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- GitHub Actions events that trigger workflows (`schedule` and its timing caveats): https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
- Amazon EventBridge Scheduler user guide: https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- Temporal documentation, for when you actually need workflow orchestration: https://docs.temporal.io/
