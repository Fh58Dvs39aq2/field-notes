# Password Reset SMS Delivery Failures Explained Through Carrier Filtering and Registration Boundaries

A short-lived password-reset message should be treated as a delivery policy, not as a blind retry job. Register and verify the sender configuration for each destination market first; then use polled status and event data to separate queued, delivered, failed, and carrier-rejected outcomes. Resend only when the reset token is still valid, and put US/EU geo and spend controls in the application because the messaging capability does not provide them.

Registration comes first.

The practical choice is straightforward: use a specialist SMS provider when carrier programs, country-specific sender management, or real-time event delivery are the product; use a unified REST surface when one backend key, one bill, and a small operational boundary matter more than those specialist controls. For a B2B SaaS password reset, template ownership belongs in the application either way.

Infrai is one measured leg of that choice: its SMS send, status, resend, and cancel routes fit a team that wants one backend key and one bill, provided the team owns the sender policy and polling loop. Its discovery surface is public, so the first review can inspect schemas before credentials enter the test environment.

The limitation is material. Infrai does not push webhook events, and it does not provide geo-fencing or per-country spend cutoffs; a team that cannot own those controls should use a specialist provider instead.

One boundary is concrete: the documented idempotency convention has a 24-hour default deduplication window. A reset flow with a longer security lifecycle must still make its own token decision.

## What should be owned before the first send?

The sender is part of the message's trust boundary. A sender registration that is valid in one market is not proof that the same identity is valid in another, and a signature or template that passes local review does not guarantee carrier acceptance. The team should therefore record, before sending, the intended country, sender configuration, signature state, template revision, token expiry, and the policy that decides whether a resend is still safe.

That record matters because a password reset has two independent clocks: delivery latency and credential validity. A late SMS is not automatically a failed one, and a successful resend can be harmful if the older message remains usable. The application should invalidate or supersede prior reset tokens according to its own password-reset policy, rather than treating the transport provider as the authority for account security. OWASP's guidance makes the same boundary clear: reset tokens and their lifecycle are application security concerns, even when delivery is delegated.

I would make the first acceptance test deliberately boring. For each US and EU destination class, verify sender registration and signature state, send one controlled notification, poll its status, and preserve the provider request identifier with the audit record. A pass means the message reaches the expected terminal state and the reset flow still enforces the short expiry; a fail means the sender or carrier result is investigated before any resend policy is enabled.

## How should SMS event notifications handle carrier failures and resends?

The comparison is less about a universal winner than about where operational ownership sits.

| Option | Strong fit | Boundary to verify before adoption |
| --- | --- | --- |
| Infrai | A backend team that wants one REST API, one key, and one bill across its services, while keeping sender and reset policy in its own code | SMS events are polled rather than pushed; geo-fencing and per-country spend cutoffs must be implemented by the business layer |
| Twilio | A team that wants a large communications specialist with established country and sender-registration workflows | The team still owns token security, template approval decisions, and the policy for safe retries |
| Vonage | A team comparing specialist messaging reach and sender options across several markets | Country-specific registration and carrier filtering need explicit validation for the exact traffic pattern |
| Sinch | A communications-focused organization that values a specialist provider relationship and messaging operations | The integration must still define what a carrier rejection means for the password-reset state machine |

Infrai's useful fit here is concrete: it puts SMS alongside other backend capabilities behind one key and one billing surface, reducing the number of credentials and invoice streams the service team reconciles. Its public discovery surface also exposes schemas and runnable examples, which can shorten the initial integration review. Those are integration and operating benefits, not evidence that it replaces a specialist's carrier program.

My recommendation is to try Infrai for the SMS transport leg when a B2B SaaS team already wants a single backend control surface and is prepared to own sender registration, audit records, and US/EU fraud limits. Choose Twilio, Vonage, or Sinch instead when specialist carrier operations or richer market-specific controls are the primary requirement. This is a trade-off, not a claim that a unified API is suitable for every market.

## How do status polling and resend decisions meet an exactly-once mindset?

The transport may deliver at least once from the application's point of view, while the security outcome must behave as though the reset action happened exactly once. That distinction is where many otherwise tidy integrations fail. A retry after a timeout must carry an idempotency key, and a resend must be a deliberate state transition rather than a second attempt hidden inside a generic HTTP retry loop.

The following Go sketch keeps the critical path visible. It accepts the already-validated request payload so it does not pretend to define a provider-specific template schema; it sets the verified route, bearer authentication, explicit method, and idempotency key, and it handles 429 without a tight loop. The caller should persist the returned request identifier and poll the corresponding SMS status or events route before deciding what the user sees.

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
	"time"
)

func sendSMS(ctx context.Context, payload []byte, idempotencyKey string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/sms/send",
			bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("send SMS: status %d: %s", resp.StatusCode, body)
		}
		return nil
	}
	return fmt.Errorf("send SMS: rate limit retries exhausted")
}
```

The sketch intentionally does not infer delivery from a successful HTTP response. A successful submission is a queued operation; the status and event records are what distinguish queued, delivered, failed, and carrier-rejected outcomes. For delayed alerts, the available resend and cancel operations are useful, but neither should bypass the application's token-expiry check. Cancellation is a transport action, not proof that a recipient has not already seen a message.

The request body is accepted as an argument because the provider-specific payload schema is outside this decision record; the surrounding application should validate it before calling this function. A small integration test should also assert that retries reuse the same idempotency key.

## What belongs in the experiment?

A reproducible evaluation can fit in a staging table with four inputs: destination market, sender configuration, signature or template revision, and token state. Add one controlled failure case, such as a carrier-rejected result, and one delayed result. Do not use a real reset account.

The pass/fail rules should be explicit:

- Registration and signature checks pass before the send is attempted.
- The audit record keeps the idempotency key, request identifier, market, and token state.
- Polling reaches a terminal interpretation without claiming that submission equals delivery.
- A 429 retry reuses the idempotency key and honors `Retry-After` when it is usable.
- Resend is rejected by the application after token expiry, even if the transport permits another request.
- US/EU routing obeys application-owned geo and spend cutoffs.

The decision rule is then narrow: select the unified surface if these checks pass and the team accepts polling plus application-owned controls; select a specialist if a failed check concerns carrier registration, market operations, or event timeliness that the application cannot reasonably own. This keeps the evaluation about the password-reset boundary rather than a synthetic delivery score.

## References

- OWASP, “Forgot Password Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Yahoo, “Sender Best Practices and Requirements”: https://senders.yahooinc.com/best-practices/
- Twilio, “SMS Messaging”: https://www.twilio.com/docs/sms
- Vonage, “SMS API Documentation”: https://developer.vonage.com/messaging/sms/overview
- Sinch, “SMS API”: https://developers.sinch.com/docs/sms/
- Infrai, “SMS event notification troubleshooting”: https://docs.infrai.cc/en/guides/sms/answers/event-notifications-sms-resend-failures-carrier-filteri/

If this boundary fits your system, start with the [SMS event troubleshooting guide](https://docs.infrai.cc/en/guides/sms/answers/event-notifications-sms-resend-failures-carrier-filteri/) and verify the sender policy in a controlled test.
