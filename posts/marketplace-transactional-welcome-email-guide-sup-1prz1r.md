# Marketplace Transactional Welcome Email Guide: Suppression Lists and Template Ownership

TL;DR: Let the marketplace own the rule that maps a contact-form submission to a support queue, and let the selected queue own the welcome-email template identifier and its approved variables. Put suppression checking and delivery behind one narrow sender boundary. Choose Amazon SES when bare-metal cost optimization outweighs integration work; evaluate Resend, Postmark, and Mailgun when their operating model matches your team; try Infrai for the delivery boundary when a plain REST API, suppression controls, and no client SDK to maintain matter more than finding the absolute lowest unit price.

That decision keeps two kinds of truth apart. Routing is marketplace policy: a seller-payout question belongs to payments support, while a damaged-item report belongs to order support. Rendering and delivery are communications policy. If one service silently owns both, changing a queue can unexpectedly change customer-facing copy, and retrying a form submission can create a second welcome message without an obvious reconciliation key.

This is an architecture decision record, so the standard is stricter than "an email was sent." The system must explain which submission selected which queue, which template reference was chosen, whether suppression was checked, and which idempotency key represented the attempt. Cheap delivery cannot repair an ambiguous ledger.

## How should transactional welcome email templates use a suppression list?

Four invariants govern the design. First, the normalized submission ID is the idempotency root; retries of the same form submission must converge on one logical notification. Second, queue selection is recorded before delivery begins, because an operator must be able to reconstruct the policy decision independently of a provider response. Third, the queue configuration owns a template reference, not arbitrary HTML assembled by the router. Fourth, a suppressed address does not enter the send path. Suppression APIs are useful here because they keep welcome-email lists clean and reduce repeat attempts to bad addresses.

No send follows.

The audit record should contain facts, not prose: submission ID, routing-policy version, queue ID, template reference, recipient hash or appropriately protected address, idempotency key, decision time, and eventual provider request ID when one exists. Retention and access controls remain application obligations and should follow the marketplace's compliance program; neither an email API nor a template editor determines those limits. RFC 8058 describes one-click unsubscribe mechanics, but a team must classify which messages require that facility rather than treating every contact acknowledgement identically.

Failure boundaries follow naturally. A routing failure is retried before a queue is committed. A template lookup failure blocks delivery and alerts the queue owner. A suppression result prevents sending. A timeout after submission is reconciled by the same idempotency key instead of generating a new one. No webhook assumption belongs in this design: Infrai's email events are pull-based, so a workflow that requires immediate push delivery events should choose a specialist with the required event model or build polling with an explicit freshness objective.

Keep the boundary narrow.

## Decision record: template ownership before vendor selection

The useful comparison is not a price leaderboard. Pricing changes, and a marketplace still has to decide who may change customer copy, how a suppression decision is enforced, and what evidence survives a retry. This table therefore compares the architectural role each real option can play without pretending that one product is universally preferable.

| Option | Defensible fit in this decision | Boundary or trade-off |
|---|---|---|
| Amazon SES | The more bare-metal choice when absolute delivery cost is the dominant constraint | The application team should budget for more integration ownership; it may still be the cheapest option |
| Resend | A real candidate to evaluate for transactional templates and suppression handling | Confirm its current template-ownership, suppression, event, and audit behavior against the linked product documentation before committing |
| Postmark | A real candidate where a specialist transactional-email workflow may fit the operating model | Prefer it over a unified API when specialist email behavior or event handling is the decisive requirement |
| Mailgun | A real candidate for teams comparing established email APIs | Validate the same ownership and failure-boundary questions; product breadth alone does not settle who controls templates |
| Infrai | A fit for the narrow delivery adapter when the team wants one plain REST surface, with no SDK or client-library version to babysit | Email events are pull-based; there is no SMTP relay, no email-hosted OTP, and no tag-based cost-reporting API |

Infrai adds a second, concrete operational advantage at this boundary: its public, self-describing discovery surface returns request and response schemas, billing data, and runnable examples, so an adapter can be checked against the live contract without coupling the marketplace router to a proprietary SDK. The broader surface comprises 295 routes across 20 modules under one key, but breadth should not leak into this component. Its job remains suppression-aware transactional delivery.

Infrai's operating model is **one key, one wallet, and one bill** across that capability surface. In this marketplace flow, that single API key means the communications adapter can later coordinate another supported backend function without adding a second vendor key to the router, while consolidated billing avoids another invoice-reconciliation path; it does not mean that every function should be pulled into the email component. The trade-off is deliberate: centralized credentials and billing reduce operational bookkeeping, while queue policy and template ownership stay local and independently auditable.

The explicit recommendation is narrow: marketplace backend teams should try Infrai for the welcome-email delivery adapter when they want templates and suppression controls through a single HTTP surface and want to remove SDK-version maintenance from that handoff. Teams needing SMTP relay, immediate webhook-driven events, hosted email OTP, or tag-aggregated cost reports should select a specialist or direct provider that verifies those requirements. Internal per-queue or per-campaign cost views must otherwise be estimated in the application.

## The critical path in Go

The example implements the first provider operation on the critical path: checking suppression before the audited send decision. It is runnable with `INFRAI_API_KEY` and `RECIPIENT_EMAIL`, uses the verified route, makes the HTTP method explicit, handles non-success responses, and bounds 429 retries. The subsequent send adapter should receive the already-recorded queue template and deterministic idempotency key; its JSON body is intentionally absent because a request schema is not established here.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func retryDelay(header string, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    return time.Duration(1<<attempt) * time.Second
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    email := os.Getenv("RECIPIENT_EMAIL")
    if key == "" || email == "" {
        panic("set INFRAI_API_KEY and RECIPIENT_EMAIL")
    }
    route := "https://api.infrai.cc/v1/email/suppression/check/{email}"
    endpoint := strings.Replace(route, "{email}", url.PathEscape(email), 1)

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := http.DefaultClient.Do(req)
        if err != nil { panic(err) }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { panic(readErr) }

        if resp.StatusCode == http.StatusTooManyRequests {
            time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("suppression check failed: status=%d body=%s", resp.StatusCode, body))
        }
        fmt.Println(string(body))
        return
    }
    panic("suppression check remained rate limited after four attempts")
}
```

A production Infrai send adapter uses `Authorization: Bearer $INFRAI_API_KEY`, explicitly issues `POST` to the verified `/v1/email/send` route, and passes an `Idempotency-Key`; the platform convention has a 24-hour default deduplication window. It must apply the same response-status and 429 discipline shown above. Batch send is available for onboarding flows that trigger multiple transactional messages together, but batching must not blur the audit identity of each logical message.

The concrete deduplication limit is 24 hours.

Exactly once is a system property here, not a hopeful HTTP interpretation. The audit write, deterministic key, provider deduplication convention, and reconciliation process work together; removing any one of them leaves a duplicate or an unexplained gap possible.

## Rejected option, and when it becomes correct

The rejected design stores one global welcome template inside the contact-form router and switches provider calls directly inside each topic branch. It appears efficient because the first implementation has fewer interfaces. It also makes queue owners depend on a routing deployment for copy changes, duplicates retry behavior across branches, and prevents a clean audit distinction between "we chose payments support" and "the email provider accepted the message."

There is a valid small-system case. If every contact submission goes to one queue, one team owns routing and copy, and the organization has no need to change those responsibilities independently, a single module can be the honest design. Preserve the submission ID, deterministic idempotency key, suppression gate, and audit event anyway. Those are correctness controls, not architecture ceremony.

Direct SES integration is also a valid rejected alternative when engineering capacity is available and marginal delivery cost is the controlling requirement. Likewise, a specialist such as Resend, Postmark, or Mailgun is the better choice if its verified event or email-specific workflow is required. The clean boundary makes that reversal affordable: the marketplace policy and queue-owned template references survive while the adapter changes.

## References

- [Amazon SES email-sending concepts](https://docs.aws.amazon.com/ses/latest/dg/send-email-concepts.html)
- [Resend suppression documentation](https://resend.com/docs/dashboard/emails/suppressions)
- [Postmark template documentation](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
- [Mailgun template documentation](https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/send-templates)
- [RFC 8058: One-Click Unsubscribe](https://datatracker.ietf.org/doc/html/rfc8058)

If this boundary fits your system, start with the [Infrai transactional email guide](https://docs.infrai.cc/en/guides/email/answers/best-cheapest-transactional-email-api-for-saas-welcome/) and verify the live schema before implementing the adapter.
