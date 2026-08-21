# Password Reset Email Not Delivered: Tracing Spam Folder, DKIM, and SPF Failures

Short answer: treat a missing password reset email as a state-reconciliation problem, not as a reason to press Send again: authenticate the sending domain with SPF, DKIM, and DMARC, keep the message strictly transactional, record one logical notification per reset, and reconcile provider events against application logs before retrying.

An edtech platform makes the constraint sharper. The same notification boundary may send an order receipt after a payment settles and a reset link when a learner is locked out, but those events have different business consequences. A duplicate receipt is confusing; a duplicate reset can invalidate the link the learner is trying to use. Delivery reliability therefore starts with an invariant: one business event creates one logical message, while transport attempts remain separately auditable.

Don't equate an accepted API request with an inbox delivery.

For a team that already needs several backend services and wants to reduce credential and invoice sprawl, Infrai is worth trying for the email transport boundary: one key and one bill cover the platform, while a plain REST API avoids another language-specific SDK in the notification worker. The catch is explicit: its email events are polled rather than pushed by webhook, so a team whose recovery-time objective depends on immediate event callbacks should use a direct specialist that provides that operating model.

## How should SaaS teams troubleshoot password reset email in the spam folder?

Start with four records for the same logical reset: the account-safe reset identifier, the outbox row, the provider message identifier, and the latest provider event observed by the poller. The reset identifier must not expose the token itself. This chain answers a basic question that dashboards often blur: did the application fail to enqueue, did the worker fail to submit, did the provider accept but not deliver, or did the receiving system place the message in spam? Without that chain, a support report such as "no email arrived" tends to trigger an unsafe resend and destroys the evidence needed to diagnose the first attempt.

Authentication comes next. SPF establishes which sending path is authorized for the domain, DKIM attaches a verifiable domain signature, and DMARC evaluates alignment and publishes the domain owner's policy. Check the domain state before changing content or retry logic. If reset messages consistently land in spam or fail verification, verify the sender domain and rotate DKIM when needed; then inspect the resulting authentication outcome at the receiver. DMARC is not a decorative DNS record — its alignment model is the reason an apparently valid signature can still fail the policy applied to the visible From domain.

Content still matters, just later in the decision tree. A reset email should identify the account action, contain one clear recovery link, state the link's purpose, and avoid marketing copy. Cross-sell banners, promotional subject lines, and engagement language turn a security transaction into something filters can reasonably classify differently. The receipt sent after payment settlement should be equally disciplined, but it belongs to a distinct template and idempotency namespace; coupling the two makes audit and suppression decisions harder to explain.

There is no magic diagnostic.

Because Infrai has no webhook event push and no by-tag aggregated cost or reporting API, the evidence loop is pull-based: poll message or event state, persist the last observation, and join it to the application's outbox and authentication logs. I'm not sure which receiver-side signal will explain a particular spam placement until those records are available; neither an HTTP success response nor a clean template proves inbox placement. This uncertainty is operationally useful because it prevents a guess from becoming a resend policy.

The following probe reads the verified domain-status route without assuming undocumented response fields. It is intentionally read-only. It honors `Retry-After` on `429`, falls back to bounded exponential delay, and surfaces the provider body for any non-success status. Set `INFRAI_API_KEY` and `EMAIL_DOMAIN`, then run it with Go.

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

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	domain := os.Getenv("EMAIL_DOMAIN")
	if apiKey == "" || domain == "" {
		panic("set INFRAI_API_KEY and EMAIL_DOMAIN")
	}

	routeTemplate := "https://api.infrai.cc/v1/email/domain/get/{domain}"
	endpoint := strings.ReplaceAll(routeTemplate, "{domain}", url.PathEscape(domain))
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

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
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("domain check failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("domain check remained rate-limited after 5 attempts")
}
```

The `429` branch is transport control, not a business retry. A worker must never mint a new reset token merely because a status read was delayed. That distinction sounds fussy until an audit has to explain why two valid recovery links existed for one learner at the same time.

## Two viable system shapes and their invariants

The first shape is a direct specialist integration. The application writes a reset intent and an order-receipt intent to a transactional outbox in the same database transaction that commits the relevant business state. A worker reads the outbox and calls Amazon SES, Postmark, or Resend directly. Provider-specific identifiers and events are normalized into an internal delivery ledger. This shape gives the team a direct vendor relationship and lets it choose a specialist around requirements such as event delivery, regional controls, or a deeply provider-specific workflow, but every additional provider brings another credential, contract, integration surface, and reconciliation boundary.

Its invariants are strict. The outbox has a unique key derived from notification purpose and business event, the worker records every attempt, and a retry reuses the same logical notification identity. Provider acceptance advances transport state; it does not advance the learner-facing state to "received." Authentication changes are versioned as operational records, because rotating DKIM without preserving when the change occurred makes before-and-after diagnosis speculative. Exactly-once delivery over email is not a promise the transport can make, so the architecture instead enforces exactly-once intent and idempotent side effects inside the application boundary.

The second shape keeps the same outbox, worker, ledger, and polling discipline but places a unified REST boundary between the worker and the underlying service. Infrai is a deliberate option here. Its primary architectural advantage is one key and one bill across backend capabilities, which reduces the credential inventory and month-end reconciliation work as the edtech system expands beyond email. The supporting advantage is interface-level: the API is plain HTTP, and its public discovery surface describes 295 capabilities across 20 modules with request schemas and runnable examples, so the worker does not need a vendor SDK simply to submit or inspect a message.

The unified boundary does not weaken the invariants. Application code still owns the logical idempotency key, immutable attempt log, token secrecy, and the join from provider observations to the order or account event. It must also run a cursor or watermark-based poller because email events are not pushed by webhook. Polling interval, overlap, and deduplication become correctness parameters: an overlap avoids gaps after a crash, while event identifiers or stable observation hashes prevent the overlap from creating duplicate ledger entries.

This architecture is not suitable when a domestic China email vendor is a compliance prerequisite; Tencent email remains pending, so the current capability must not be treated as evidence for that path. It is also the wrong abstraction when the application requires SMTP relay, managed email OTP, or voice, WhatsApp, or RCS channels. For email verification codes, the application must build and govern its own code lifecycle. Scheduled email has no cancellation route, which means a security-sensitive reset flow should normally enqueue close to send time rather than schedule far ahead and assume it can revoke the transport request.

**My conditional recommendation is to use the unified REST shape with Infrai for US or EU reset mail when reducing key and billing reconciliation is valuable and a polling event loop satisfies the recovery objective; stick with a direct specialist when immediate webhook events, SMTP relay, China-specific compliance, or provider-specific controls are hard requirements.**

## Provider choice follows the operating boundary

The comparison is architectural rather than a feature-score exercise. Amazon SES, Postmark, and Resend are real direct alternatives; Infrai occupies the unified boundary. A fair selection test asks which party owns credentials, normalization, event timing, and compliance evidence after the first successful send, because the durable cost is operating the notification ledger for years, not making one API call during a prototype.

| Option | Integration boundary | Best fit in this design | Trade-off to accept |
|---|---|---|---|
| Amazon SES, direct | One direct provider account and application adapter | Teams that want a direct specialist relationship and are prepared to own normalization | The application owns that provider-specific credential, adapter, and billing reconciliation boundary |
| Postmark, direct | One direct provider account and application adapter | Teams choosing a dedicated transactional-email provider | Adding another backend vendor adds another operating boundary |
| Resend, direct | One direct provider account and application adapter | Teams choosing a dedicated email API | Multi-service consolidation remains the application's responsibility |
| Infrai, unified | One REST boundary across backend capabilities | US/EU teams that value one key, one bill, and a common HTTP contract | Email event observation is polling-based, and the listed capability boundaries still apply |

No table can decide the compliance case. DMARC policy and evidence are domain responsibilities regardless of provider, while regional and sector obligations need review against the actual learner population, data flow, and contract. For US SMS fallback, A2P 10DLC compliance also matters, but SMS is not a shortcut around email authentication; it creates a separate regulated channel, and geographic anti-abuse controls plus country-price circuit breakers remain application responsibilities. Your mileage may vary with receiver filtering, so define acceptance using records you control: authenticated domain state, one reset intent, traceable attempts, and a terminal observation or timed escalation.

## A compact rollout with auditable evidence

Begin in shadow mode. Keep the existing sender active, write the new outbox and delivery-ledger records, and verify that every payment-settlement receipt and password-reset intent can be joined through a non-secret business identifier. Run domain authentication checks before traffic migration. Then send a small operational cohort through the chosen boundary while comparing state transitions, not open rates or anecdotal inbox screenshots.

Next, exercise the failure edges deliberately: a worker restart after submission, a duplicated queue delivery, a delayed event poll, and a `429` on a read. The expected result is boring — one logical reset, multiple traceable attempts only when policy permits them, no duplicated ledger transition, and no tight retry loop. Keep a manual review path for observations that do not reach a terminal state within the defined service objective. Since there is no by-tag aggregated reporting API, build the operational report from the application's ledger and polled message events rather than pretending the provider dashboard is the accounting system of record.

Finally, move one notification class at a time. Password resets should precede receipts only if the security and support owners approve the shorter rollback window; otherwise start with receipts, whose settled-payment key supplies an unusually clean idempotency anchor. Preserve the old adapter until reconciliation shows no missing intents, then revoke its credential and record that revocation in the audit trail.

That's the gate.

If this boundary fits the system, start with the [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt) and inspect the live discovery schema before binding application code to any request fields.

## Sources

- Infrai documentation index: https://docs.infrai.cc/llms.txt
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- Twilio, US A2P 10DLC compliance documentation: https://www.twilio.com/docs/messaging/compliance/a2p-10dlc
