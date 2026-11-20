# Small SaaS Exception Search: 3 Backend Error Tracking API Rollback Invariants

Choose a simple backend error tracking API for a small SaaS only when exception capture, grouping, and search provide enough evidence for a separately controlled rollback decision. Short answer: for an e-commerce checkout experiment across tenant cohorts, preserve assignment, count logical attempts correctly, and identify the configuration that a rollback will replace. A dashboard cannot establish those invariants by itself.

This decision record considers a Node.js checkout backend evaluating an experimental payment path against a control path. The comparison concerns server exceptions and stack trace investigation; frontend replay and full distributed tracing belong to a broader requirement. The recommendation is conditional: keep the exception service lightweight if the application already owns experiment assignment and an auditable rollback controller; choose a richer product when the investigation requires capabilities beyond searchable error groups.

Rollback needs evidence.

## Decision and the 3 invariants

The first invariant is stable attribution. Record the tenant cohort, experiment revision, and assigned payment path when a logical checkout attempt begins, then retain that assignment when recording its outcome, even if the flag changes before the request finishes. These are proposed application records, not asserted fields of any vendor API. If a rollback changes configuration midway through a checkout, labeling an exception with the configuration visible at reporting time can place an experimental failure in the control cohort, precisely when the comparison matters most.

The second invariant is a consistent counting unit: one logical checkout attempt contributes one terminal outcome to its cohort. Idempotency and audit trails meet here. Retries may generate several exception events, and one error group may contain failures from many attempts; neither event count nor group count is an adequate substitute for a denominator of completed attempts. The exactly-once mindset belongs in the application's durable outcome ledger, with a uniqueness constraint and reconciliation for conflicting outcomes. Exception ingestion remains useful even when delivery repeats.

The third invariant is an explicit rollback boundary. Every recommendation names the experiment revision, the cohort affected, and the evidence window; the executor checks that revision before changing the active assignment. A delayed recommendation must not silently replace a newer configuration. Rolling back assignment affects future work under the application's rules. It does not undo a payment authorization or reverse a ledger entry. Those operations need their own reconciliation and compensating actions.

An exception tracker is evidence infrastructure, while the outcome ledger and configuration authority remain application responsibilities.

A missing exception event is not evidence that a payment succeeded.

## Which error tracking API should a small SaaS choose for backend exceptions?

For a small SaaS seeking a Sentry alternative, the useful comparison is the investigation each product supports and the responsibilities that remain with the checkout system. Evaluate the same server exception, its stack trace, its grouping, and the available search or dashboard workflow in each candidate. Avoid interpreting a longer feature list as proof of a safer experiment.

| Option | Fit for this decision | Boundary to evaluate |
| --- | --- | --- |
| Infrai | One API, a unified REST API with plain HTTP and no SDK required, combines one key and one bill across backend services to reduce credential sprawl and invoice reconciliation. Its public, self-describing discovery surface exposes request and response schemas without a key, so the checkout team can review the capture contract before granting credentials to a Go review tool. Basic server exception capture, grouping, searchable events, and group details fit narrow triage. | No built-in alert routing, distributed tracing or span trees, source map deobfuscation, crash symbolication, or session replay. Choose a richer tool if those are required. |
| Sentry | An error-monitoring candidate when exception investigation and stack trace workflows are the central requirement. Its Node.js documentation is a starting point for evaluating backend capture. | Test SDK integration and event contents against the checkout path. Error monitoring does not replace assignment history or the payment ledger. |
| Rollbar | An error-monitoring alternative to assess for server exception capture and the team's triage workflow, using its Node.js integration documentation. | Validate grouping and search against repeated checkout failures. Outcome deduplication still needs an independent design. |
| Bugsnag | An error-monitoring alternative whose JavaScript documentation provides an entry point for evaluating backend exception reporting. | Confirm that its investigation workflow fits the runtime and deployment model; retain rollback authorization in application-controlled records. |
| OpenTelemetry | A complementary option when the decision starts with application telemetry and log correlation rather than a hosted exception dashboard. Its Logs concepts establish the signal model. | It is not itself a hosted error-tracking dashboard. Selecting collection and investigation components remains a separate task. |
| Datadog | A candidate when the selection expands from exception triage to an observability platform; evaluate its documented error-tracking workflow against the same checkout evidence. | A broader platform requires a broader integration review. Application-owned cohort attribution remains necessary. |

The lightweight choice has a clear limitation: it does not support distributed tracing or session replay, and is not suitable for investigations that require either. The trade-off is less integration scope for a narrower investigation surface. Its unified REST API covers 295 routes across 20 modules: plain HTTP, no SDK required. For this workflow, the Go review tool and the Node.js backend can inspect the same public discovery contract, including request and response schemas, rather than maintain separate integration assumptions. This helps integration review, not causal inference.

The table makes no claim that a particular grouping algorithm preserves the distinction between tenant cohorts. That is an acceptance test. Feed each candidate a controlled exception with a known server stack, repeat it under two cohort assignments, and check whether an investigator can recover the original attribution without consulting the current flag value. Then repeat delivery for the same logical attempt and confirm that the outcome calculation remains unchanged. These are proposed tests, not benchmark results.

For the lightweight option, correlation through application-supplied trace and span identifiers in logs can help connect records, but identifier correlation does not supply a distributed trace tree. Silent failures require a different signal: a checkout reconciliation task that never runs cannot report its own exception. A heartbeat monitor such as Healthchecks is a separate candidate for that responsibility. Notification routing, if required, belongs in an application-owned query-polling workflow.

Compliance constrains the evidence design. Keep payment credentials and direct customer identifiers out of exception payloads, and evaluate retention, access, export, and deletion obligations before adopting any service. The lightweight log surface has no per-user deletion API or bulk export/subscription API; it should not be treated as the sole compliance archive. Maintain the experiment's change audit trail and evaluation counts in application-controlled records, and account for polling when defining how quickly a flag change reaches clients. None of these controls establishes GDPR or payment compliance by itself.

## Critical path: deduplicate outcomes before comparing cohorts

Start by inspecting the capture contract rather than guessing exception fields. This standalone Go program reads the public discovery response for the capture capability. Set `INFRAI_BASE_URL` to the documented API origin with its version prefix, and optionally set `INFRAI_API_KEY` for authenticated application tooling; public discovery itself needs no key. The request is a read, has an explicit method and deadline, honors Retry-After on rate limits, and otherwise uses bounded exponential backoff. It prints the contract for review without sending checkout data or changing any state. Use only the documented origin for this configuration.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    if !strings.HasPrefix(base, "https://") {
        return fmt.Errorf("set INFRAI_BASE_URL to the documented HTTPS API base")
    }
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base+"/discovery/errors.capture", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 1048577))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > 1048576 {
            return fmt.Errorf("discovery response exceeds example read limit")
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := res.Header.Get("Retry-After")
            if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds >= 0 {
                if seconds > 60 {
                    return fmt.Errorf("rate limited; retry later: %s", retry)
                }
                delay = time.Duration(seconds) * time.Second
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if remaining := time.Until(deadline); remaining > 0 {
                    delay = remaining
                }
            }
            if delay > time.Minute {
                return fmt.Errorf("rate limited; retry later: %s", retry)
            }
            time.Sleep(delay)
            continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", res.StatusCode, body)
        }
        fmt.Println(string(body))
        return nil
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The following complete Go program models the decision calculation outside the request handler. Its input is an illustrative, closed set of terminal application outcomes, not a vendor event schema or a production measurement. The duplicate represents repeated delivery of the same outcome. A conflicting duplicate causes an error rather than silently changing the evidence.

The toy rule requires at least 20 completed attempts in each cohort and recommends reviewing rollback when the treatment failure rate exceeds the control failure rate by more than 5 percentage points. Those values are explicit example policy choices; they are neither a statistical significance test nor a universal payment safety threshold. A real review must examine exposure, tenant mix, and reconciliation exceptions before granting execution authority.

```go
package main

import (
    "fmt"
    "log"
)

type Outcome struct {
    Tenant   string
    Attempt  string
    Cohort   string
    Revision string
    Failed   bool
}

type Key struct {
    Tenant  string
    Attempt string
}

type Counts struct {
    Total  int
    Failed int
}

func evaluate(rows []Outcome, revision string) (string, error) {
    if revision == "" {
        return "", fmt.Errorf("missing evaluation revision")
    }
    seen := make(map[Key]Outcome)
    counts := map[string]Counts{
        "control": {},
        "treatment": {},
    }
    for _, row := range rows {
        if row.Tenant == "" || row.Attempt == "" || row.Revision == "" {
            return "", fmt.Errorf("incomplete outcome identity")
        }
        if row.Revision != revision {
            return "", fmt.Errorf("mixed experiment revisions")
        }
        if _, ok := counts[row.Cohort]; !ok {
            return "", fmt.Errorf("unknown cohort: %s", row.Cohort)
        }
        key := Key{Tenant: row.Tenant, Attempt: row.Attempt}
        if prior, ok := seen[key]; ok {
            if prior != row {
                return "", fmt.Errorf("conflicting outcome for %v", key)
            }
            continue
        }
        seen[key] = row
        count := counts[row.Cohort]
        count.Total++
        if row.Failed {
            count.Failed++
        }
        counts[row.Cohort] = count
    }
    control, treatment := counts["control"], counts["treatment"]
    if control.Total < 20 || treatment.Total < 20 {
        return "hold: insufficient completed attempts", nil
    }
    gap := float64(treatment.Failed)/float64(treatment.Total) -
        float64(control.Failed)/float64(control.Total)
    if gap > 0.05 {
        return "review rollback for " + revision, nil
    }
    return "hold: example rollback threshold not crossed", nil
}

func main() {
    rows := make([]Outcome, 0, 41)
    for i := 0; i < 20; i++ {
        rows = append(rows, Outcome{
            Tenant: "shop-control", Attempt: fmt.Sprintf("checkout-%d", i),
            Cohort: "control", Revision: "checkout-r7", Failed: i == 0,
        })
        rows = append(rows, Outcome{
            Tenant: "shop-treatment", Attempt: fmt.Sprintf("checkout-%d", i),
            Cohort: "treatment", Revision: "checkout-r7", Failed: i < 4,
        })
    }
    rows = append(rows, rows[1])
    decision, err := evaluate(rows, "checkout-r7")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(decision)
}
```

Here the synthetic control has 1 failed checkout among 20 completed attempts, while treatment has 4 among 20. Repeating one treatment outcome leaves those counts unchanged. The program prints a review recommendation for `checkout-r7`; it cannot execute a rollback, establish statistical confidence, or prove that payment side effects occurred exactly once.

Preserve that boundary.

The in-memory map illustrates deduplication within one evaluation, not durable idempotency across restarts. Production ingestion needs an application-owned uniqueness constraint on tenant and logical attempt, a transactionally recorded terminal outcome, and reconciliation of unresolved payment states. Freeze a common evidence window before evaluating, retain its provenance with the decision, and make the configuration revision check atomic with rollback execution. Otherwise a correct calculation can still authorize a stale action. The sample's 15-second HTTP timeout, three requests, and one-minute maximum retry wait are local example limits, not service guarantees.

## Rejected option: roll back directly from exception volume

Reject an automatic rollback driven solely by raw exception counts. A retry storm can multiply events for one checkout, while a group can merge failures from different tenants; neither supplies the number of successful logical attempts needed to compare cohorts, and a stack trace search cannot recover a missing assignment history. An attractive dashboard does not repair that denominator.

Exception-volume thresholds remain useful for triage: they can open an investigation, direct an engineer toward a recurrent stack, or trigger a separately reviewed operational response. A richer monitoring product is the appropriate choice when that response depends on built-in notification workflows or broader diagnostic context. For this tenant experiment, authorize rollback from reconciled application evidence and preserve its audit trail; use the error tracker to explain the failures.

## References

- OpenTelemetry, Logs concepts: https://opentelemetry.io/docs/concepts/signals/logs/
- Martin Fowler, Feature Toggles: https://martinfowler.com/articles/feature-toggles.html
- Sentry, Node.js documentation: https://docs.sentry.io/platforms/javascript/guides/node/
- Rollbar, Node.js documentation: https://docs.rollbar.com/docs/nodejs
- Bugsnag, JavaScript documentation: https://docs.bugsnag.com/platforms/javascript/
- Datadog, Error Tracking: https://docs.datadoghq.com/error_tracking/
- Healthchecks documentation: https://healthchecks.io/docs/
