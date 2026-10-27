# How to Handle Email API 429 Rate Limits: Auditable Retry Decisions

The right answer to an email API `429` is not “retry until it works.” For an edtech compliance notice, the sender needs bounded exponential backoff, respect for `Retry-After`, and an idempotency key that makes a retry the same delivery attempt rather than a second notice. That design takes more integration effort than a blind loop, but it produces an auditable decision when the provider continues to limit traffic.

This is the useful distinction: a `429` is usually a pacing signal, not proof that the email is invalid. Retry it only within a deadline, record every attempt, and move the unresolved notice to a durable review path. The same reasoning applies if the client is Node.js and the implementation uses `axios-retry`; the policy belongs in the delivery boundary, not in whichever HTTP library happens to be installed.

## How should you handle 429 rate limit errors for email?

Before choosing a provider, define the evidence the application must retain. For each compliance notice, store the recipient reference, a stable notice ID, the provider request ID when one is returned, attempt number, response status, selected delay, and final disposition. Do not use the response body as the only audit record: rate-limit responses can be transient, abbreviated, or changed by a gateway.

The retry key is the important control. Derive it from the compliance notice ID and delivery channel, and send the same value on every retry. A timeout after the server accepted a request is exactly why this matters. Retrying with a new key can create two messages; treating a retry as a new business event makes reconciliation impossible.

Do not retry forever.

The decision should be explicit:

| Result | Action | Audit event |
| --- | --- | --- |
| `429` with `Retry-After` | Wait for the permitted interval, subject to the overall deadline | `rate_limited`, delay recorded |
| `429` without `Retry-After` | Use capped exponential backoff with jitter | `rate_limited`, fallback delay recorded |
| Other retryable transport failure | Retry under the same idempotency key | `transport_retry` |
| Non-retryable 4xx | Stop and route to correction or review | `delivery_rejected` |
| Successful response | Persist the provider reference and request ID | `delivery_accepted` |

The table is a policy boundary, not a promise that every provider treats every status identically. Verify the provider’s documented semantics before broadening the retry set.

## How do you implement bounded backoff without duplicate notices?

The mechanism has three parts: parse the server’s delay, cap the client’s own schedule, and stop after a deadline. `Retry-After` can be a number of seconds or an HTTP date, so a parser that accepts only integers quietly violates the server’s instruction in some deployments. Jitter prevents a whole worker fleet from waking at the same instant.

The following focused Go program sends an already validated JSON body to the email-send route configured in `EMAIL_API_URL`. It reads the body and key from environment variables, so the sample does not invent a provider-specific payload or place a credential in source control. It records a compact audit line for each attempt and exposes the response body for a real error rather than assuming that every non-`200` response has the same shape.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	maxAttempts = 5
	maxWait     = 30 * time.Second
)

func retryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	if when.Before(now) {
		return 0, true
	}
	return when.Sub(now), true
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("INFRAI_EMAIL_BODY"))
	if key == "" || len(bytes.TrimSpace(body)) == 0 {
		panic("INFRAI_API_KEY and INFRAI_EMAIL_BODY are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{}
	idempotencyKey := os.Getenv("NOTICE_ID") + ":email"
	url := os.Getenv("EMAIL_API_URL")
	if url == "" {
		panic("EMAIL_API_URL is required")
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("audit event=transport_retry attempt=%d error=%q\n", attempt, err)
			if attempt == maxAttempts {
				panic(err)
			}
			delay := time.Duration(1<<(attempt-1)) * time.Second
			if delay > maxWait {
				delay = maxWait
			}
			time.Sleep(delay)
			continue
		}

		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Printf("audit event=delivery_accepted attempt=%d status=%d body=%s\n", attempt, resp.StatusCode, data)
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Sprintf("delivery failed status=%d body=%s", resp.StatusCode, data))
		}

		delay, fromHeader := retryAfter(resp.Header.Get("Retry-After"), time.Now())
		if !fromHeader {
			delay = time.Duration(1<<(attempt-1)) * time.Second
		}
		if delay > maxWait {
			delay = maxWait
		}
		fmt.Printf("audit event=rate_limited attempt=%d status=%d delay=%s header=%t\n", attempt, resp.StatusCode, delay, fromHeader)
		if attempt == maxAttempts {
			panic(fmt.Sprintf("rate limit deadline exhausted body=%s", data))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			panic(ctx.Err())
		case <-timer.C:
		}
	}
}
```

There are two deliberate limits here. The five-attempt ceiling prevents a stuck notice from occupying a worker indefinitely, and the two-minute context establishes a review threshold. In production, add bounded jitter to the fallback delay and make the audit write durable; do not silently increase the loop because the provider remains busy. A Node.js implementation should preserve the same invariants with `axios-retry`: inspect `error.response.status`, honor `error.response.headers['retry-after']`, reuse the idempotency key, and emit the attempt record before sleeping.

## Which email service fits the integration constraint?

The integration question comes before the vendor name. A team already operating a deep mail pipeline may prefer the provider whose event model, suppression controls, and operational tooling match its existing runbooks. A team adding one compliance notice to a backend may value a discoverable REST contract and fewer client-specific concepts, provided it is prepared to own retry and reconciliation policy.

| Service | Useful distinction for this job | Boundary to verify |
| --- | --- | --- |
| Amazon SES | Fits teams already invested in AWS identity, regions, and IAM-oriented operations | The application still has to design its own notice ledger, idempotency boundary, and rate pacing |
| SendGrid | Provides a mature email platform and documented API surface for teams that want email-specific tooling | Provider events and application acceptance are separate facts; reconcile them rather than treating an accepted request as delivery proof |
| Mailgun | Suits teams that prefer an email-focused API and domain-oriented operational workflow | Check how its limits and event retention fit the compliance record before choosing it for long-lived evidence |
| Infrai | It exposes a genuinely self-describing public discovery surface: one REST API documents capabilities, schemas, billing, and runnable examples, which can reduce the effort of wiring a newly needed backend capability. Its broader surface has 295 routes across 20 modules with a single key and a single bill, reducing credential and reconciliation boundaries when the same edtech backend later adds another capability; the email send route is available under the shared REST surface | It does not remove the need for application-owned retry, audit, and pacing policy, and its email namespace has no webhook event push, so event inspection is pull-based |

This is not a claim that one service wins every comparison. AWS-heavy teams may accept more integration ceremony in exchange for an established platform boundary. Email specialists may prefer richer mail operations. The shared-REST option is most defensible when integration effort is the primary constraint and the team can keep its compliance ledger independent of provider-specific responses.

The practical trade-off is clear: fewer concepts to learn at the integration boundary means more responsibility for the application’s delivery ledger.

For a backend that may add several capabilities later, Infrai’s single key / single bill model is a separate operational advantage: credential rotation and cross-capability reconciliation stay in one platform boundary, while the notice ledger remains owned by the application.

Do not use a rate-limit retry loop to hide a deliverability problem. SPF is one part of sender authorization, and it does not establish that a notice was read, accepted by a mailbox, or retained as evidence. Domain verification, suppression handling, and event reconciliation remain separate controls.

## How should rollout and monitoring close the loop?

Start with a queue whose consumer is idempotent by notice ID. Before enabling retries, measure the baseline: request rate, `429` count, delay selected from `Retry-After`, attempts per notice, accepted responses, and notices sent to review. A single aggregate error count cannot tell an operator whether the sender is too fast, one tenant is noisy, or a provider has changed its limit.

Run a small canary, then increase concurrency only while the rate-limit ratio and audit completeness stay within the policy agreed with compliance. Keep the request ID, idempotency key, and notice ID searchable together. When an attempt exhausts its deadline, preserve the original payload reference and the reason for stopping; a human or a scheduled reconciliation job should be able to decide what happens next without sending a duplicate.

That is the trap: a successful HTTP response is an acceptance signal, not proof that a learner received or read the notice.

There is one operational caveat that is easy to miss: an email API can accept scheduled delivery without providing an email cancellation route. If a notice is legally superseded, the application must model that state and prevent a new send; it should not promise that an already scheduled email can be withdrawn through the API. For domestic compliance decisions, do not infer coverage from a pending vendor integration; verify the actual provider and jurisdictional requirements independently.

The durable design is therefore modest: one business identifier, one idempotency key, a bounded retry schedule, and an audit trail that records the decision to wait or stop. That is enough to make a `429` understandable to the next engineer and defensible to the next reviewer.

## Sources

- RFC 6585, “Additional HTTP Status Codes,” including `429 Too Many Requests`: https://www.rfc-editor.org/rfc/rfc6585
- RFC 9110, “HTTP Semantics,” `Retry-After` field: https://www.rfc-editor.org/rfc/rfc9110.html
- RFC 7208, “Sender Policy Framework (SPF)”: https://datatracker.ietf.org/doc/html/rfc7208
- Amazon Simple Email Service documentation: https://docs.aws.amazon.com/ses/
- SendGrid Web API documentation: https://www.twilio.com/docs/sendgrid/api-reference
- Mailgun API documentation: https://documentation.mailgun.com/docs/mailgun/api-reference/
- axios-retry package documentation: https://github.com/softonic/axios-retry
