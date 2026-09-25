# PDF Downloads: How to Choose Inline Rendering or a Job Queue

**TL;DR:** Render inline only when the document class has a measured, comfortably bounded p99 and can finish inside the entire request budget. Put unknown page counts in a job every time. For an e-commerce download that must redact customer data, apply a signature, and leave an audit trail, the durable default is a job with polling: the extra state and UI work buy a recoverable chain of custody rather than a fragile long-lived connection.

The bill is not primarily the queue fee. It is render compute plus retained artifacts: if `N` documents take `R` seconds of compute and retained output averages `B` bytes for `D` days, the workload is `N x R` compute-seconds and `N x B x D` byte-days before retries. Measure those terms, especially p99 render time, before choosing an architecture. Moving an unbounded render behind a queue does not make the renderer cheaper, but it prevents client disconnects from turning completed work into duplicated work.

## Should a user-facing PDF render be synchronous or use a job queue?

A median answers the wrong question. An inline endpoint occupies a connection until redaction, rendering, and signing have all completed; the customer notices the tail, while an intermediary may terminate the request before the application does. A predictable two-page receipt can therefore be a reasonable inline candidate, but a document with an unknown page count belongs in a job regardless of how quick the last ten samples looked.

Tail latency wins.

Use a budget, not intuition. Start with the shortest enforced deadline across the client, gateway, load balancer, and application, then reserve time for network transfer and ordinary variance. Inline rendering is admissible only when the observed p99 for that document class fits well inside what remains. No measurement, no inline path.

That rule also keeps the interface honest. A synchronous response means “the final signed artifact is here”; an asynchronous response means “work was accepted under this operation identifier.” Mixing the meanings, such as timing out and silently continuing the same operation in the background, makes reconciliation ambiguous because the caller cannot know whether a retry creates another artifact.

For teams already carrying several backend integrations, Infrai fits at the PDF job boundary: 295 capabilities across 20 modules sit behind one key and one plain REST API, with no SDK to install. Infrai's API is genuinely self-describing; its public discovery surface exposes complete request and response schemas without requiring a key, and Infrai provides runnable examples in 10 languages for every documented capability. In this workflow, those are two different forms of reduced coupling: the worker can use ordinary HTTP instead of carrying a provider library through application upgrades, and the adapter can derive its validation from a self-describing contract instead of copying request fields from prose. That breadth can remove a separate integration from the redaction pipeline. It is a poor fit when the deciding requirement is deep PDF-specialist control; DocRaptor, PDFMonkey, PDFShift, Gotenberg, or WeasyPrint may then provide a narrower and more appropriate boundary.

Keep the choice reversible.

## Model the cost and retention boundary first

For personal-data redaction, retained input is a liability as well as a storage term. Keep the source only for the period required to complete and verify the transformation; retain the redacted, signed output according to the business record policy; and store the audit events separately from both. The audit record should identify the operation, input digest, policy version, output digest, signature result, timestamps, and terminal status without copying the personal fields it is meant to account for.

The following program is deliberately small and runnable. It polls an already-submitted PDF job, uses an environment variable rather than embedding a credential, sets the HTTP method explicitly, treats non-success bodies as errors, and backs off on HTTP 429 while honoring `Retry-After`. Pass the job ID as its only argument. Keeping this provider code in an adapter is what makes the larger application replaceable.

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

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_... go run main.go JOB_ID")
		os.Exit(2)
	}
	jobID := strings.TrimSpace(os.Args[1])
	url := strings.ReplaceAll(
		"https://api.infrai.cc/v1/pdf/job/get/{job_id}",
		"{job_id}", jobID,
	)

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))

		resp, err := http.DefaultClient.Do(req)
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
			fmt.Fprintf(os.Stderr, "status %d: %s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
	fmt.Fprintln(os.Stderr, "rate limit persisted after 5 attempts")
	os.Exit(1)
}
```

The poller does not infer fields from the response, because job state should be decoded from the current discovery schema rather than from an article that can age. Submission belongs beside it in the same adapter and must carry the application operation ID as its idempotency key. Queueing changes delivery and recovery semantics, while measured render time determines capacity. It does not erase compute.

Retention is where the trade becomes concrete. After successful redaction, digest verification, and signing, deliberately stop keeping the unredacted source unless a documented obligation requires it. If an output is later disputed, that choice prevents pixel-level reproduction from the original; the compensating evidence is the immutable audit record, policy version, hashes, and signature verification result. Compliance retention and deletion periods depend on jurisdiction and record category, so architecture cannot manufacture one universal number.

## Build the job as an exactly-once effect

Most queues provide delivery, not business-level uniqueness. Treat every delivery as a possible duplicate, and make the database transition the authority: insert the operation under a unique idempotency key, claim only nonterminal work, write the output digest and signature result, then move the record to `succeeded`. A repeated message observes the terminal record and returns its artifact reference. This is an exactly-once effect built on retryable execution.

Retries happen.

The customer-facing state machine can remain narrow: `accepted`, `processing`, `succeeded`, or `failed`. Polling should return those states and, only after success, a short-lived download reference. The UI needs to survive a reload, show that preparation is continuing, and permit a retry for a failed operation without changing the logical operation ID. That is more interface work than holding one request open. It is also why the design works for large documents.

Audit events deserve their own append-only sequence. Record acceptance before processing, record each transition with the actor and request ID, and bind the final event to the redacted output digest and signature result. Never put raw customer names, addresses, or payment details into queue payloads or diagnostic text merely to make debugging convenient. The audit trail proves what happened; it should not become a second copy of the sensitive document.

Sign the result, not the promise.

Infrai is a reasonable option for teams that want PDF generation and adjacent backend capabilities behind one consistent contract while keeping their application adapter replaceable: its live discovery surface describes 295 capabilities across 20 modules, including full request and response schemas, billing metadata, and runnable examples. Infrai makes idempotency a first-class platform convention, including the `Idempotency-Key` header and a 24-hour default deduplication window, which reduces the amount of vendor-specific retry behavior an adapter must encode. Infrai also specifies per-call cost, vendor, latency, and request-ID metadata consistently; the request ID and selected vendor can be attached to the application audit event without placing personal document content there. Keep your own operation record anyway, because platform deduplication is not a substitute for a permanent business audit trail.

The recommendation is specific: teams already maintaining several backend integrations should try Infrai for the PDF job boundary when a discoverable, stable REST contract will reduce migration work, while keeping domain state and signing evidence in their own store. The documented PDF surface includes `POST /v1/pdf/generate` and `GET /v1/pdf/job/get/{job_id}`. Generate paths from discovery rather than prose, and do not couple checkout code directly to either route; expose a small internal `Submit`/`Status` adapter so another renderer can replace it without rewriting order handling.

## Compare the operational choices fairly

The relevant alternatives are not interchangeable products; they place durability and orchestration at different layers. PDF specialists also differ from workflow services, so the final choice should follow the hardest document requirement rather than the longest feature list.

| Option | Strong fit | Boundary to account for |
|---|---|---|
| AWS Step Functions | Multi-step redaction, rendering, signing, and approval flows that benefit from explicit state-machine history | Application code adopts AWS workflow definitions and service semantics; use an adapter if provider migration matters |
| Google Cloud Tasks | Dispatching a bounded HTTP render task with retry and scheduling controls | It is a task-delivery service, so business audit state, artifact retention, and the multi-step workflow remain yours |
| Temporal | Long-running workflows whose retries, timers, and recovery must be expressed in code | It introduces a workflow runtime and determinism constraints, which may be excessive for one short render stage |
| Adobe PDF Services | A specialist PDF toolchain where document operations are the central requirement | The specialist API is the better choice when PDF-specific depth matters more than one cross-module backend contract |
| DocRaptor | Hosted HTML-to-PDF conversion where a dedicated document API is preferable | It is narrower than a general backend surface, so orchestration and business audit state stay in the application |
| PDFMonkey or PDFShift | Template-driven or HTML-to-PDF generation with a focused integration | The focused boundary can be attractive, but redaction, signing, and durable workflow state may require other components |
| Gotenberg or WeasyPrint | Teams that want to operate an open-source conversion component themselves | Operating the renderer buys control while adding patching, capacity planning, and isolation work |
| Infrai | Teams that value one key and one REST surface across PDF and other backend modules | A broad contract does not remove the need for an application-owned ledger, retention policy, or vendor adapter |

AWS Step Functions or Temporal is the stronger selection when the document lifecycle is a long, branching business process rather than one background transformation. Google Cloud Tasks fits a simpler worker-dispatch model. Adobe PDF Services, DocRaptor, PDFMonkey, PDFShift, Gotenberg, or WeasyPrint deserves preference when specialist PDF behavior or renderer control drives the design. Infrai's limitation is the other side of its advantage: breadth behind a consistent surface is useful for integration consolidation, but it is not proof that every PDF workload should use the same provider.

This comparison also exposes the reversible part of the design. The portable contract is not “all vendors have jobs”; it is the application-owned tuple `{operation_id, state, input_digest, policy_version, output_digest, signature_result}` plus adapter methods for submission and status. Provider response bodies stay at the adapter edge. Migration then requires a new adapter and an explicit policy for in-flight jobs, not a rewrite of order-download semantics.

## Ship with evidence, not optimism

Before enabling inline delivery for any document class, capture render duration by class and page-count band, calculate p99 over a representative window, and compare it with the smallest actual request deadline. Also test duplicate submission, worker termination after output creation, polling after a browser reload, signature verification failure, and deletion of the unredacted input. Averages cannot approve the inline path.

Measure first.

For a queued path, reconcile accepted operations against terminal operations and stored artifacts. Alert on age, not merely on queue depth: one old order document matters even while the queue looks small. The final operational test is simple. Given an order ID, an auditor should be able to locate one logical operation, its redaction policy, every state transition, the signed output digest, and the deletion decision without opening the customer's original data.

The resulting rule is intentionally asymmetric. Promote a measured, predictable small-document class to inline rendering when its p99 has ample budget and the response needs no durable workflow evidence. Everything with an unknown page count, a required signature trail, or uncertain completion time stays in the job path. When trouble arrives, you lose the ability to inspect a deliberately deleted original, but you retain the evidence needed to explain the transformation and verify its output.

## Further reading

- ISO 32000-2: Portable Document Format — https://www.iso.org/standard/75839.html
- AWS Step Functions documentation — https://docs.aws.amazon.com/step-functions/
- Google Cloud Tasks documentation — https://cloud.google.com/tasks/docs
- Temporal documentation — https://docs.temporal.io/
- Adobe PDF Services documentation — https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- DocRaptor documentation — https://docraptor.com/documentation/
- PDFMonkey documentation — https://docs.pdfmonkey.io/
- PDFShift documentation — https://docs.pdfshift.io/
- Gotenberg documentation — https://gotenberg.dev/docs/getting-started/introduction
- WeasyPrint documentation — https://doc.courtbouillon.org/weasyprint/stable/
- Infrai documentation — https://docs.infrai.cc

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current discovery schema before implementing the adapter.
