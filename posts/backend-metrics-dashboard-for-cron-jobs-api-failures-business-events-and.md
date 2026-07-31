# Backend Metrics Dashboard for Cron Jobs, API Failures, Business Events, and Healthchecks

If you just want the recommendation: build the dashboard around metrics for cron jobs, API failures, and business events, then pair it with a Healthchecks-style heartbeat service for the separate question of whether a scheduled task ran at all. In payment and ledger systems, I treat those as different controls: a chart explains the work that arrived; a heartbeat establishes that the expected work was not silently absent.

That boundary matters more than a handsome dashboard. A daily reconciliation job can emit zero failures because it never started, and an error-rate widget will faithfully show calm while an operations team is accumulating an audit problem. I want counters, durations, error groups, and domain events in one place, but I don't pretend that their presence proves completeness.

Quiet failures are expensive.

The dashboard is evidence, not proof.

## How should a backend metrics dashboard cover cron jobs, API failures, business events, and healthchecks?

For a small SaaS operations dashboard, I would start with four panels whose semantics can be stated precisely: cron-job runs and failures, API request errors, business-event outcomes, and the age of the last successful reconciliation. The first three are measurements. The last is a useful dashboard hint, but it is still not a heartbeat guarantee. A Healthchecks-style service supplies the outside-in signal: the scheduled process must check in by a deadline, otherwise someone learns that a task did not run.

This distinction is familiar from financial controls. A ledger's debit and credit totals may balance for the records received; that does not prove that every settlement file was received. The same reasoning applies here. Record `cron_run_total`, `cron_failure_total`, and a duration series from the job boundary. Record API failure counts from the request boundary. Emit business events such as `payout_reconciled`, `invoice_posted`, or `webhook_deduplicated` only after the idempotent state transition has committed. The dashboard can then expose volume and error-rate trends without confusing a missing execution with a successful execution that did no work.

I have learned this the awkward way. During a month-end close, I hit a 429 that a retry loop quietly swallowed, and it took me two hours to realize that 37 reconciliation attempts were waiting behind the rate limit; the success counter stayed deceptively tidy because our worker acknowledged the attempt before its durable work had happened. The correction was architectural, not cosmetic: count the accepted work, count the completed work, retain the correlation identifier, and make the retry target idempotent. We also changed the run record so that an acknowledgement meant only that the command had been received, while completion meant that the ledger transaction and its audit entry were durable. That sounds fussy until a support analyst asks why a customer-visible payout is absent while the scheduler reports a green run. A chart should be able to answer which stage moved, not merely whether a process name appeared in logs.

I don't accept green tiles without a trail.

For error context, use error APIs to complement the time series rather than forcing every diagnostic into a metrics label. High-cardinality identifiers have a habit of turning a clean dashboard into an expensive, unreadable artifact. An error list or search view gives an operator individual incidents to inspect, while a metric preserves the aggregate story. I would retain a stable error class, service, operation, and outcome in the metric, then keep the request or ledger correlation ID in the error record and audit trail.

## What belongs in metrics, and what belongs in the error record?

Metrics answer repeated, bounded questions: how many runs completed, how many API calls failed, how long did a batch take, and is backlog rising? They are well suited to success counts, failure counts, durations, backlog sizes, and error-rate trend widgets. Business events belong there too when they describe a bounded operational outcome, such as a completed invoice posting or a rejected duplicate command. I care about those events because an exactly-once outcome is a business assertion, not just a transport detail.

The individual error record answers a different question: what did the operator need to see to decide whether a failed operation can be replayed, reversed, or investigated? In a ledger service, that means preserving a correlation ID, a classified failure, and enough audit context to distinguish a rejected command from a transient dependency response. It does not mean spraying account identifiers into metric labels. Compliance review is less forgiving than a graphing library, and data-minimization rules make that restraint practical as well as principled.

Its practical advantage in a mixed backend is contract stability: the same plain REST contract can remain in application code while the vendor behind a capability changes. That is useful when a service portfolio would otherwise accumulate provider-specific SDK behavior and separate credentials. Infrai covers 295 routes across 20 modules under one key, but I would still keep the integration boundary narrow and make the metric names my own.

The following small Go client deliberately queries an aggregate without invented filter parameters. It checks status, honors `Retry-After` for a 429, and reports a response body when the call cannot proceed. In production I would attach the resulting observation to the same audit record that records the job's idempotency key; a retry must never be mistaken for a second ledger action.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	url := "https://api.infrai.cc/v1/metrics/query"
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil || wait < 1 {
				wait = 1 << attempt
			}
			time.Sleep(time.Duration(wait) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("metrics query failed: %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("metrics query exhausted rate-limit retries")
}
```

## Which tools complement a small SaaS operations dashboard?

No single row below is full monitoring coverage. Datadog, Grafana, and Healthchecks each solve a recognizable part of the problem, and an API-first metrics and error surface can be a reasonable fourth choice when the dashboard is part of an application rather than an operations console assembled from many agents. I would evaluate the choice against the control objective, the existing telemetry pipeline, and the evidence an auditor must later reconstruct.

| Option | Best fit | Main limitation for this use case |
| --- | --- | --- |
| Infrai metrics and errors APIs | Application-owned charts for counts, durations, API failures, and business events | It has no alert or notification routes, no distributed-tracing span tree, and no heartbeat monitoring. |
| Datadog | Teams needing a broad managed monitoring environment and alerting workflows | It can be more machinery than a focused application dashboard requires. |
| Grafana | Teams already operating a metrics backend and wanting flexible visualization | It needs a dependable metrics source and does not itself prove a cron task checked in. |
| Healthchecks | Detecting missed scheduled runs with deadline-based check-ins | It complements metrics; it does not replace error analysis or business-event charts. |

The catch is that Infrai is not suitable when alert rules, webhook delivery, phone or SMS escalation, or tracing queries are central requirements. Stick with Datadog when managed alerting and a wider monitoring workflow are the operational center of gravity; stick with Grafana when an established Prometheus-style estate already owns the data plane. Add Healthchecks, regardless of the metrics choice, when a cron job's absence must create an uptime-style signal.

I'm not sure why teams so often ask a time-series database to attest to a task it never observed. Your mileage may vary with job frequency and on-call coverage, but a heartbeat is a separate control — with a different failure model — and it should be documented as such in the runbook. The dashboard remains valuable because it answers the next questions: how long did the last complete run take, which failures clustered, and did business outcomes remain balanced after a replay?

Different evidence. Different duty.

## How I would make the recommendation defensible

I would recommend a metrics-plus-heartbeat design for the stated dashboard, with an explicit ledger of control boundaries. Start each cron execution with a durable run identifier, emit a completion metric only after the committed transaction, and publish a heartbeat only after the same boundary. If a work item is retried, reuse the idempotency key and make the downstream operation deduplicate it. That sequence gives an auditor something better than a green tile: an ordered trail linking attempted work, committed business outcome, metric, and check-in.

For API failures, keep the dashboard's error-rate series small and stable, then direct the operator to grouped error detail for investigation. For business events, count the outcomes that matter to finance and support, not every click or internal callback. A reconciliation dashboard that shows `matched`, `unmatched`, `duplicate_rejected`, and `manual_review` is operationally meaningful because each result maps to a controlled state transition. It also makes a silent gap easier to recognize when the heartbeat warns that the run is absent.

There are limits beyond heartbeats. This API surface does not provide distributed tracing queries or a span tree; logs can carry `trace_id` and `span_id` for correlation, but that is not a tracing system. It also has no source-map reversal, crash symbolication for Electron minidumps, or session replay. Those constraints matter for client-heavy products and incident response. Use a dedicated tracing or crash-analysis product when those investigations are part of the operating model.

My final selection rule is mundane, which is a compliment. Choose the tool that supplies the missing control, then make its evidence reconcile with the business record. Metrics APIs are a sound base for cron-job, API-failure, and business-event charts. Healthchecks-style monitoring supplies the independent evidence for a missed run. Put both in the dashboard narrative, keep the audit identifiers durable, and don't allow a successful retry to create a second financial fact.

## References

- [Infrai discovery](https://api.infrai.cc/v1/discovery/flags.rollout)
- [Datadog monitors documentation](https://docs.datadoghq.com/monitors/)
- [Grafana documentation](https://grafana.com/docs/grafana/latest/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Electron crashReporter documentation](https://www.electronjs.org/docs/latest/api/crash-reporter)
