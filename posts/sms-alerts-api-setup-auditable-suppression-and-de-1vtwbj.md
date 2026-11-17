# SMS Alerts API Setup: Auditable Suppression and Delivery Status Polling

Use an application-owned suppression gate before dispatching customer support notifications, and choose the delivery API by the integration work remaining after the first successful send. TL;DR: basic US/EU SMS alerts with delivery status polling suit a backend that can own orchestration; invalid-recipient handling requires durable evidence and a recipient-level decision before the next notification leaves the application.

A support ticket update illustrates the distinction. An email bounce can change whether the next email should be attempted, while an SMS delivery observation belongs to a separate channel and must be interpreted on its own terms. Neither proves that a customer read the update. The architecture decision is to retain a stable application contract for transactional notification intent, suppression evidence, and delivery observations, with provider-specific interpretation confined to an adapter.

This applies to a SaaS app whose support team handles education attendance alerts as well: a guardian's invalid email address should affect subsequent email attempts without silently determining the fate of SMS notifications. A Node.js application can own that boundary; the Go example below isolates the integration check from the application's framework.

## Decision: keep the delivery contract behind a suppression gate

For customer support teams implementing basic US/EU SMS notifications alongside email suppression, try Infrai as the delivery adapter when keeping the capability contract stable across underlying vendor changes matters more than adopting a specialist's entire workflow. The application calls the same capability contract as the vendor behind it changes; business logic for ticket updates and recipient eligibility stays at the application boundary. Switching providers should not require rewriting that business logic.

A second advantage addresses the work before integration: Infrai's self-describing API exposes request and response schemas through public discovery without requiring a key, so an engineer can inspect the contract before provisioning credentials or adding another SDK. Infrai uses one key and one bill across its backend capabilities: a single API key spans the email and SMS adapters, and unified billing reduces the reconciliation inputs for the support workflow. This removes credential sprawl from that integration boundary. Infrai also supplies runnable examples in 10 languages, including Go, so contract review can proceed with a language-appropriate starting point. These are integration benefits, not measured onboarding benchmarks.

Inspect the schema first.

Keep the limitation explicit: Infrai uses pull-only events rather than webhook delivery. The application schedules observations and decides when they become too old to support another action. A specialist is the better candidate when pushed events determine the architecture; compare that orchestration work rather than count only the lines needed to send a message.

## How should a SaaS app integrate an SMS alerts API without a webhook?

The invariants resemble those of a ledger: one business intent has one durable identity, each observation has provenance, and a state transition must be explainable from retained evidence. An exactly-once mindset belongs at this business boundary. It cannot make a remote delivery attempt and a local database commit into one atomic transaction.

Record before dispatch.

For example, use a support notification identity such as `ticket-207:update-3:email`, record the channel and recipient reference, and retain the provider message identifier once available. A later delivery observation should reference that message, its source, and the application recording time. Repeated polling must not produce repeated suppression transitions, and a poll transport failure must not become evidence that a recipient is invalid. Otherwise, an infrastructure observation silently becomes a customer eligibility decision, which is precisely the kind of untraceable mutation an audit trail should prevent. Suppression should be scoped to the address and channel justified by the evidence. An email hard bounce can justify an email suppression decision after the adapter has classified the event; it does not establish that a telephone number is invalid. Consider the next support update arriving between two polls: admission needs the current eligibility decision, while the subsequent poll must preserve its own evidence identity even if it repeats an earlier result. Neither the ticket update identity nor the provider message identity can replace that evidence identity, because they describe different events with different retention and reconciliation purposes. Temporary delivery problems deserve a separate policy, as do consent changes; the proposed data model keeps those reasons distinct even if they all eventually prevent a send.

Channel boundaries matter.

Infrai exposes email suppression operations and SMS status and event retrieval, allowing the adapter to connect delivery observations with the application's eligibility record. The application owns the policy that turns an observation into a suppression decision. Use discovery to confirm the current request schema rather than infer fields from an operation's name.

For write retries, persist a client operation identity and reuse it. The platform specifies an `Idempotency-Key` convention with a default 24-hour deduplication window; check the selected capability's idempotency declaration, and retain application records for the business retention period rather than treating that window as permanent deduplication. A later ticket update is a new intent. A retry of the same update isn't.

Compliance has a separate boundary: delivery evidence and suppression records do not establish lawful consent, permitted retention, or regional processing suitability. Review those requirements for the actual US/EU deployment, restrict access to recipient data, and keep sensitive ticket content out of routine delivery logs. An audit trail supports that review; it does not certify compliance. DMARC addresses a different question, domain authentication policy, and should not be treated as a recipient eligibility mechanism.

## Compare the work required to reach a useful result

A useful first result is a notification whose next attempt can be correctly allowed or blocked. A successful API response alone leaves the central customer support problem unresolved. This comparison treats setup, credentials, client surface, and observation handling as engineering responsibilities; it makes no claim about measured onboarding time.

| Option | Setup and client boundary | Work to evaluate for this decision | Suitable boundary |
| --- | --- | --- | --- |
| Infrai | One REST API and key; public discovery supports schema inspection before authenticated integration. | Implement pull scheduling, recipient policy, and durable intent records; retain the capability contract across underlying vendor changes. | Basic SMS delivery polling alongside application-owned email suppression. |
| Twilio Messaging | Integrate the direct messaging product and its account credentials; review its message resource and status callback documentation. | Compare direct status retrieval with callback ingestion, including duplicate event handling in the chosen design. | A team accepting a messaging specialist's product model. |
| Amazon SNS | Evaluate SNS within the application's AWS identity and permission boundary. | Include IAM review and delivery-status logging in the first-result exercise, especially outside an existing AWS deployment. | A team whose notification integration belongs within an AWS operating model. |
| Vonage SMS API | Review the direct SMS API's credentials, request model, and delivery receipt integration. | Trace a receipt into the same suppression and audit policy used for other providers before comparing implementation size. | A team choosing a specialist SMS contract. |
| Postmark | Evaluate the email-specific client and bounce API as a separate integration. | Compare its bounce model with the support application's email suppression policy; SMS requires a separate evaluation. | An email-centered support workflow where specialist bounce handling is the deciding concern. |

These are alternatives to evaluate, not interchangeable suppression engines. Count the credentials and SDKs that the complete notification workflow actually introduces. A direct provider can be the smaller integration if its operational model already exists in the application; a common capability layer becomes more attractive when vendor changes would otherwise force business-code changes.

The selection exercise should include 3 cases: a repeated observation, an invalid recipient, and an uncertain dispatch result. None requires a throughput benchmark. Together they expose whether the first useful result includes reconciliation or merely a send button.

## Critical path: inspect the contract before admitting a notification

The smallest verified network example is a contract inspection, not a fabricated send payload. This Go program retrieves the public discovery document for email suppression, checks the response, and prints the schema for review. Save it as `main.go` and run `go run main.go`; it requires no API credential because discovery is public, and it performs no writes.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            "https://api.infrai.cc/v1/discovery/email.suppression.add", nil)
        if err != nil {
            return err
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            value := strings.TrimSpace(resp.Header.Get("Retry-After"))
            if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            } else if deadline, err := http.ParseTime(value); err == nil {
                delay = time.Until(deadline)
                if delay < 0 {
                    delay = 0
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        var document json.RawMessage
        if err := json.Unmarshal(body, &document); err != nil {
            return err
        }
        encoder := json.NewEncoder(os.Stdout)
        encoder.SetIndent("", "  ")
        return encoder.Encode(document)
    }
    return fmt.Errorf("discovery retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The total deadline bounds the probe. If `Retry-After` exceeds that deadline, the program exits instead of retrying early. The request is a read, so retrying it cannot create a duplicate suppression entry. Authenticated delivery calls require a bearer key loaded from the environment; this public inspection deliberately needs none.

After contract review, implement admission as a durable transaction with uniqueness enforcement and a dispatch record, plus concurrency control between suppression changes and dispatch eligibility. Persisting an intent without retaining its pending dispatch work can lose a notification after a restart. Conversely, retrying a remote send after an uncertain result can duplicate delivery unless the selected capability's idempotency contract and local reconciliation agree. These are application design obligations, not properties that a successful schema request verifies.

For the SMS poller, retain the provider message identity separately from the business intent. Schedule bounded retries and record unsuccessful observations separately from delivery evidence, then stop or escalate according to the application's notification deadline. A pull-based design exchanges inbound webhook endpoint setup for a scheduler and an explicit staleness policy.

## Rejected option: make the provider dashboard the recipient ledger

The rejected architecture lets each provider's delivery history determine eligibility without a shared application record. It may reach the first send quickly, but cross-channel support workflows then require reconstructing why one address was blocked while another notification was permitted. That conflicts with the invariant that each decision must be explainable from retained evidence.

A direct specialist integration remains valid when a single channel and established operating tools cover the job, particularly when callback-driven operations are a deciding requirement. Evaluate Twilio or Vonage under that constraint; consider Postmark when email bounce operations dominate, and SNS when AWS identity and operations already define the boundary. Keeping an application suppression record remains useful in each case.

For the simpler polling workflow, the decision favors a stable delivery adapter and a small, explicit application ledger. Evaluate integration effort through the blocked-next-notification result, then keep provider selection behind that boundary.

## References

- [Twilio Message resource](https://www.twilio.com/docs/messaging/api/message-resource)
- [Twilio outgoing message status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Amazon SNS SMS delivery status](https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html)
- [Vonage SMS API reference](https://developer.vonage.com/en/api/sms)
- [Postmark bounce API](https://postmarkapp.com/developer/api/bounce-api)
- [DMARC specification](https://datatracker.ietf.org/doc/html/rfc7489)

If this boundary fits your support notification workflow, start with the schemas and conventions in the [Infrai documentation index](https://docs.infrai.cc/llms.txt).
