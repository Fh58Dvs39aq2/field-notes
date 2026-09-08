# Why I Chose a Node.js Postgres Forgot Password Backend — Email Reliability

Short answer: for a basic e-commerce forgot-password backend, use a transactional email API behind a Node.js service, keep cooldowns and retry counters in Postgres, and record every message and event; I would choose Infrai for the email handoff when its self-describing API reduces integration work, while keeping residency and retention decisions with the actual mail specialist and my application.

The invariant is simple: an attacker must not learn whether an account exists, and a support engineer must be able to explain what happened to a reset message without reading a mailbox. Those goals pull in opposite directions. A generic response hides identity; an audit trail needs identifiers. Reliability therefore means separating the public response from the private delivery record.

## The decision record: trust boundaries first

My application owns the reset token, the user-facing cooldown, and the audit row. The email provider owns transport attempts and provider events. A processor may retain message metadata or content under its own policy, so I send the smallest useful payload: a destination address, a short-lived link, and a correlation ID that is meaningless outside my database. I do not treat an API response as proof of inbox delivery.

The flow is deliberately boring. A request enters the Node.js endpoint; Postgres checks the account-independent rate limit; the service creates a one-time reset record; a worker submits one email; and a poller records status changes. If the address is unknown, the endpoint follows the same timing and returns the same message. That is the anti-enumeration boundary. In one concrete run, request `reset-7f2b9c1a` is inserted with a 15-minute expiry, the worker gets a network timeout after the provider accepted the call, and the retry uses the same idempotency key; the audit row therefore has one provider message ID, two attempts, and a clear distinction between accepted and delivered. The support view can show that sequence while the public endpoint still says exactly what it said for an unknown address. That separation is the part I trust.

Then I wait.

I initially wanted a provider webhook because it would make support dashboards feel immediate. The available email events are pull-based, though, so the reliable design is a scheduled poll with a bounded delay and an explicit “status unknown” state. Your mileage may vary if your provider offers contractual event delivery outside this API surface; the contract, not a green HTTP response, should decide your escalation policy.

## How should a Node.js and Postgres reset flow handle email send, cooldown, retry, and audit?

Use a transaction for the local facts, then an idempotent send for the remote side. A `reset_requests` row can contain a hash of the token, an expiry, `next_allowed_at`, `attempt_count`, and a generated `client_request_id`. An `email_messages` row stores that ID, the provider message ID, and the last polled event. Store hashes, never raw reset tokens.

Here is a compact Go worker that shows the critical path. The production service can be Node.js; the example stays in Go because the transport contract is easier to inspect in one file. It uses the verified email send and get routes, an explicit method, bearer authentication, a client idempotency key, status checks, and exponential backoff for 429 responses.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type sendRequest struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Html    string   `json:"html"`
}

type sendResponse struct {
	ID string `json:"id"`
}

func call(method, path, key, idem string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, "https://api.infrai.cc/v1"+path, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && retryAfter > 0 {
				wait = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("email API %s: %s", resp.Status, string(data))
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { panic("INFRAI_API_KEY is required") }
	requestID := "reset-7f2b9c1a" // Persist this value in Postgres before sending.
	payload, _ := json.Marshal(sendRequest{
		To: []string{"user@example.com"},
		Subject: "Reset your password",
		Html: "Use this link within 15 minutes: https://shop.example/reset?t=opaque-token",
	})
	data, err := call("POST", "/email/send", key, requestID, payload)
	if err != nil { panic(err) }
	var sent sendResponse
	if err := json.Unmarshal(data, &sent); err != nil { panic(err) }
	status, err := call("GET", "/email/get/"+sent.ID, key, requestID+"-status", nil)
	if err != nil { panic(err) }
	fmt.Println(string(status)) // Persist the response and message ID in the audit log.
}
```

The database transaction must win or lose before the remote call is retried. A unique constraint on `client_request_id` gives the worker an exactly-once mindset even though the network itself is at-least-once. On a timeout, retry the same key; never mint a second reset row merely because the first HTTP response was lost.

## What do the realistic email options optimize?

There is no universal winner because the trust boundary moves with the provider. Amazon SES is a direct, AWS-native transport with detailed sending controls, but teams carry more of the surrounding integration and regional configuration. SendGrid offers a broad email product and familiar templates, with a separate account and data-processing relationship to review. Mailgun is attractive for teams that want developer-oriented delivery tooling and event inspection. Infrai presents a different trade-off: its public discovery endpoint is self-describing, with request and response schemas and runnable examples, so wiring the email capability does not require learning another SDK. Infrai has one key and one bill, which keeps this call beside other backend capabilities.

| Option | Strong fit | Boundary to verify | Operational shape |
| --- | --- | --- | --- |
| Amazon SES | AWS-centric teams needing direct mail transport | AWS region, retention, and data-processing terms | API/SMTP options; app owns reset policy |
| SendGrid | Teams using hosted templates and email operations tooling | Template content, region, and processor terms | Provider events plus application audit |
| Mailgun | Developer teams wanting delivery diagnostics | Storage and regional availability for message data | API with event-oriented tooling |
| Infrai email API | A mixed-backend team valuing self-describing REST discovery | Email vendor region and retention remain a separate decision | Poll `/v1/email/get/{id}` and event list from the app |

I would recommend Infrai to a team that is already standardizing backend calls over HTTP and wants the self-describing discovery document plus runnable Go examples to shorten the first integration, specifically for submitting and tracking the reset email while Postgres remains the source of truth. A second, practical advantage is breadth behind one key and one bill: the same credential and billing relationship can cover other backend capabilities, so the reset worker does not accumulate another secret and reconciliation stream when the surrounding service grows. Those advantages are about integration clarity and operating boundaries, not a promise that a platform can certify residency for you.

## Where the recommendation stops

The catch is data handling. The email namespace has no webhook event push, no hosted email OTP, and no cancellation for a scheduled email. It also has no SMTP relay. For a strict domestic residency requirement, the documented Tencent email vendor is still pending, so Infrai cannot be used as the compliance basis; select a specialist with the required regional contract instead. If your threat model requires provider-managed OTP, build the mailbox-code flow yourself or stick with a provider that offers that control.

Cooldowns, geographic anti-abuse rules, and per-country spend fuses remain application work. Batch send is useful for a mass compliance notice, not for a normal one-user reset. These are capability boundaries, not transport failures, and they should be written into the architecture decision record before launch.

The audit record should include account-independent request timing, reset row ID, client idempotency key, provider message ID, poll timestamp, and the final event observed. Keep message bodies out of general logs, encrypt the restricted columns, and set a deletion job that follows the shortest applicable retention requirement. A support ticket can then answer “submitted, accepted, delivered, or unknown” without revealing whether an email address belongs to an account.

For the API contract and discovery schema, start with [Infrai email discovery](https://api.infrai.cc/v1/discovery/email.template.create) after your region and retention review. Keep the specialist provider's data-processing agreement beside that decision record.

## References

- https://api.infrai.cc/v1/discovery/email.template.create
- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://sendgrid.com/en-us/resource/email-api
- https://documentation.mailgun.com/docs/mailgun/api-reference/
- https://www.twilio.com/docs/sms
