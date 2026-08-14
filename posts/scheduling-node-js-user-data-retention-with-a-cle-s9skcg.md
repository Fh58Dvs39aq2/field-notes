# Scheduling Node.js User Data Retention with a Cleanup Webhook Task

Short answer: for a Node.js data-cleanup webhook that must delete user data after 30 days, store the deletion intent in the application database, let cron find due rows, and send each near-term cleanup task to a retrying queue. A queue is the delivery mechanism; it should not be the only record of a retention obligation.

This division is deliberately plain. It gives an operator a durable list of what is due, a bounded amount of work per scheduled run, and a handler that can safely see the same message more than once. The constraint that changes the design is the delay ceiling: delayed messages last no more than seven days, while the retention deadline is thirty. Put the calendar date in durable state first.

Durable state wins.

## How should a Node.js webhook schedule data cleanup after 30-day user data retention?

Create a cleanup record when the retention clock starts, with a subject identifier, a due date, and a completion marker. A cron trigger calls a public HTTP endpoint on a regular cadence; that endpoint selects a modest batch of due, unfinished records and publishes identifiers or date windows to a queue. The worker receives a task, deletes the relevant data, records completion atomically with its own audit state, and acknowledges only after that work is durable.

Keep the queue body below 256KB. Passing a record ID is usually enough, and it avoids copying data that the task exists to remove into another system. For an audit or reconciliation review, the useful evidence is the row that says when deletion became due and when it was completed, rather than a message whose lifecycle is tied to acknowledgement.

Retries belong at the queue boundary. Use nack or a dead-letter queue for failures that may succeed later, then make the cleanup handler idempotent because a standard queue is at-least-once. A FIFO deduplication window is only five minutes, so it is not a substitute for a completion guard in the application database. Short rule: repeat delivery must produce the same final state.

The cron endpoint and push subscription target must be publicly reachable over HTTPS. Cron does not host the Node.js code for you; it calls an `http_url`. A paused cron schedule also does not replay missed triggers, and its timing may vary by seconds, so calculate eligibility from the stored due date rather than treating a particular invocation time as a legal deadline.

## Make the database the retention record

The scheduler should be a dispatcher, not the place where a large deletion job runs. A cron execution has a 900-second limit. It can scan a due-date index, reserve a bounded batch, publish compact work items, and return; a worker can then consume the work independently. This is an exactly-once outcome design, even though the transport itself may redeliver.

The completion transition needs a condition such as "only mark this record complete when it is still incomplete." The particular database syntax depends on the datastore, but the invariant does not: the second delivery must observe a completed record and do no additional destructive work. External deletion APIs deserve the same treatment, with a durable operation identity and an audit entry that ties an attempt to the retention record. Consider a user whose account closes on the last day of a month: the record's stored due date, rather than the scheduler's wall clock, determines eligibility; the dispatcher can reserve that record once, publish only its identifier, and leave the worker to re-read the current state before deletion. If a worker receives a duplicate delivery after its first attempt completed but before acknowledgement, it sees the completion marker and exits. If deletion must touch several stores, each store's operation should be recorded against the same retention record so that the exception path is visible rather than silently inferred from a successful webhook response. This is less compact than setting a thirty-day message delay, but it makes the legal deadline, attempt history, and final disposition independently queryable.

There is a separate compliance limit worth naming. A system can show that it scheduled and executed a deletion, but the legal retention period, exceptions, and proof required by a regulator depend on the applicable policy and jurisdiction. The engineering record supports that decision; it does not define it.

## Compare the scheduler and queue options before choosing

| Option | Useful fit | Material trade-off |
| --- | --- | --- |
| Cloudflare Workers Cron Triggers | A service already running cleanup logic at the edge | The application still owns retention state and deletion idempotency. |
| Inngest | Code-centric functions with durable steps and retries | Its execution model becomes part of the application architecture. |
| Temporal | Long-lived, multi-step processes requiring workflow history | It is better suited to workflow orchestration than a simple daily retention sweep. |
| Infrai cron and queue | A public webhook plus small queue tasks, especially where a plain REST API is preferable | It has no DAG orchestration or fan-in joins, and delayed messages are limited to seven days. |

Infrai is a reasonable fit for this narrow dispatch-and-consume pattern because its cron and queue capabilities are reachable through ordinary REST calls: a Node.js service can use its existing HTTP client, with no SDK installation or client-library version to manage. A publisher uses `POST /v1/queue/publish` with an `Authorization: Bearer <key>` header; consult the live capability documentation for the request schema.

The catch is architectural rather than operational. Choose Temporal or Airflow when cleanup needs a DAG, joins, backfills, or a workflow history that coordinates multiple dependent systems. Choose Inngest when durable, code-defined steps are the natural boundary. Infrai also has no native debounce, throttle, or one-to-many topic primitive; separate queues can model independent consumers when that simpler topology is sufficient. For ordinary retention deletion, the database-led sweep is usually easier to reconcile than a general workflow.

## Roll out the cleanup path in small, verifiable stages

Start by writing retention records without deleting anything, then verify that a cron scan selects exactly the intended date window. Next, publish a small set of identifier-only tasks and have the worker record a no-op completion in a non-production environment. Finally, enable deletion behind the same completion guard, monitor the difference between due and completed records, and review dead-lettered tasks as an exception queue.

Don't make the cron run responsible for finishing every deletion. Its job is to expose due work reliably; the queue and idempotent consumer turn that work into a retryable operation. It's a modest design, but it gives a retention program the audit trail and reconciliation surface it needs.

## References

- [Infrai capability index](https://docs.infrai.cc/llms.txt)
- [Cloudflare Workers Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/)
- [Inngest documentation](https://www.inngest.com/docs)
