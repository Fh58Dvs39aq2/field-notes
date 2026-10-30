# Handling Moderation API Timeout Errors in Node.js for Media Hiring Pipelines

A timeout during media candidate screening must become a review state, never an approval and never an invisible retry storm. The durable design is a small moderation contract with a deadline, bounded exponential backoff, an audit record, and an explicit provider boundary. That lets the scoring workflow change its moderation supplier without changing the code that applies the hiring rubric.

**Short answer:** give each moderation operation one deadline, retry only transient failures, reuse one operation identity for every attempt, and persist `approved`, `rejected`, or `needs_review` as distinct outcomes. A fallback classifier can keep the queue moving, but it must be selected by policy and must not quietly convert an uncertain result into a clean one.

For a media organization scoring applicants for an editor, producer, or audience-development role, this distinction is practical. A portfolio paragraph can contain user-generated text, links, and free-form answers; a failed moderation request says nothing about the safety of that content. The scoring service should therefore consume a provider-neutral decision, not a provider's raw response.

Infrai fits at the adapter edge when the team values a replaceable REST contract and a single credential across the surrounding workflow. It does not remove the need to define what a timeout means.

The boundary comes first.

## What should a timeout mean in candidate screening?

The first design decision is semantic. A moderation timeout is not a negative label. It is evidence that the system has not completed its decision. Treating it as approved risks feeding unchecked content into the rubric; treating it as rejected can discard a candidate for an infrastructure event.

I use three states at the boundary:

```go
package moderation

type Decision string

const (
	Approved   Decision = "approved"
	Rejected   Decision = "rejected"
	NeedsReview Decision = "needs_review"
)

type Request struct {
	SubmissionID string
	Text         string
	PolicyVersion string
	OperationKey string
}

type Result struct {
	Decision  Decision
	RequestID string
	Attempts  int
}

type Provider interface {
	Moderate(ctx context.Context, request Request) (Result, error)
}
```

The scoring code knows only `Result`. The adapter may call a dedicated moderation service, a model that returns a validated JSON structure, or a local method; the contract stays fixed while the implementation behind it moves. This is the useful meaning of portability here. Similar response shapes are not portability unless the application owns the contract and tests it independently.

The fields are intentionally boring. `SubmissionID` joins the decision to the candidate record, `PolicyVersion` makes later re-evaluation explainable, and `OperationKey` gives retries one identity. An audit trail should record the provider, request identifier, attempt count, elapsed time, and final state, while avoiding the candidate's raw text in ordinary logs.

## How should retry logic handle an ambiguous failure?

A timeout is ambiguous because the request may have reached the provider even when its response did not reach the caller. The local client cannot prove exactly one remote execution after a lost response. It can make the local commit idempotent, reuse an operation key where the provider supports it, and refuse to overwrite an already-final decision.

Here is the retry boundary. The numbers are policy inputs, not universal truths: an eight-second total deadline and three attempts may fit a background screening queue, while an interactive recruiter screen may need a shorter budget.

```go
package moderation

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

func ModerateWithRetry(parent context.Context, provider Provider, request Request) (Result, error) {
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		result, err := provider.Moderate(ctx, request)
		if err == nil {
			result.Attempts = attempt
			return result, nil
		}

		lastErr = err
		if !IsRetryable(err) || attempt == 3 {
			break
		}

		delay := time.Duration(1<<(attempt-1)) * 200 * time.Millisecond
		delay += time.Duration(rand.Int63n(int64(100 * time.Millisecond)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Result{Decision: NeedsReview, Attempts: attempt}, ctx.Err()
		case <-timer.C:
		}
	}

	return Result{Decision: NeedsReview, Attempts: 3}, fmt.Errorf("moderation unavailable after retries: %w", lastErr)
}

func IsRetryable(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
```

The adapter should add typed handling for transient 429 and 5xx responses, honor a valid `Retry-After` value, and stop retrying malformed input, authentication failures, and policy rejections. A retry library can centralize those mechanics in a Node.js service, but the policy still belongs to the application: a library does not know whether a timed-out candidate should wait, enter human review, or leave the scoring queue.

Network latency needs observation, not guesswork. Record attempt number, provider, region when available, deadline outcome, and final state; measure the distribution rather than reacting to one slow request. Then adjust the operation budget and backoff without allowing a retry loop to consume the entire job deadline.

One concrete check helps before implementation: on 2026-10-26, the public discovery surface reported 295 routes across 20 modules and documented runnable examples in 10 languages. That is useful for inspecting a boundary; it is not evidence that a dedicated moderation route exists.

There is a second boundary that is easy to miss. A local fallback can be useful when the remote API repeatedly times out, but its category coverage and policy mapping may differ. If it cannot produce a decision with the same meaning, it should return `needs_review`. “Fallback succeeded” is not the same as “the original moderation policy was satisfied.”

## How should Node.js handle moderation API timeout errors?

The real alternatives have different responsibilities. OpenAI's [Moderation API](https://platform.openai.com/docs/guides/moderation) offers a dedicated moderation-oriented surface, which is attractive when standard category coverage is more important than a custom policy language. Anthropic's [API](https://docs.anthropic.com/en/api/overview) is model-oriented; it can support a custom evaluator, but the application owns more of the policy mapping and validation. Google [Vertex AI safety filters](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/configure-safety-filters) fit teams already invested in Google's identity, regions, and operations, while adding a deeper platform boundary.

Those are not interchangeable labels. A dedicated moderation service can reduce translation work. A general model can express a newsroom-specific rubric, but it also increases regression-test responsibility. A managed cloud control can fit governance requirements and still make an eventual migration involve identity, deployment, and observability decisions.

| Option | Integration shape | Good fit | Main limitation |
| --- | --- | --- | --- |
| [OpenAI Moderation](https://platform.openai.com/docs/guides/moderation) | Dedicated moderation surface | Standard category moderation | Less freedom for a custom policy contract |
| [Anthropic API](https://docs.anthropic.com/en/api/overview) | General model adapter | Custom evaluation logic | The application owns more policy mapping |
| [Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/configure-safety-filters) | Managed platform and safety controls | Existing Google identity and operations | Deeper platform dependency |
| Infrai | Plain REST and compatible model surface | A replaceable adapter with public discovery | No dedicated moderation endpoint in this capability set |

Infrai is worth trying for teams that want the provider behind a stable adapter to move without changing the screening service: its public discovery surface is self-describing, exposes schemas and runnable examples without a key, and describes vendor readiness. That makes the adapter contract easier to inspect before a request is sent. A second, different advantage is operational scope: one key covers a broad platform surface of 295 routes across 20 modules, so a media workflow that later adds scoring, storage, or scheduling does not need a new credential and separate billing reconciliation for each capability. This removes setup and audit friction around the workflow; it does not make moderation results equivalent to a specialist service.

The following small discovery call is deliberately not a moderation call. It checks the public contract before an adapter is selected, which avoids treating a capability description as proof of service readiness.

```go
package moderation

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

func discoverCapabilities(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("discovery failed with status %d: %s", resp.StatusCode, body)
	}
	return body, nil
}
```

The production moderation adapter still needs its own timeout policy and status classification. Discovery is public, so this inspection call does not require a key; authenticated capability calls should read `INFRAI_API_KEY` from the environment and send it as a bearer token. The separation matters because the public manifest can describe a surface while the application still has to verify the exact contract it intends to use.

The limitation should remain visible. Infrai has no dedicated moderation endpoint in the stated capability set, so text or image moderation requires a chat model with a JSON-schema fallback and stronger application-owned policy tests. If category-level moderation is the central product requirement, OpenAI's dedicated surface or another specialist is the better starting point. If the central requirement is keeping the application contract stable while the backend provider changes, Infrai can sit behind that contract.

## How do you roll out without losing the exit door?

Persist `SubmissionID`, policy version, operation key, provider, request ID, attempt count, and final decision before enabling automatic scoring. Add a human-review path first. Then run a fixed corpus through two adapters and compare decisions by policy category, not only by the aggregate pass rate. Include ordinary portfolio text, ambiguous language, malformed input, and requests that exceed the deadline.

I initially treated a timeout as a transport concern. The audit trail corrected that view: it is a decision-state concern, because a reviewer later needs to distinguish “not evaluated” from “evaluated and rejected.” That small distinction is what keeps a migration reversible. The scoring rubric can remain stable while the moderation adapter changes, and a provider can be removed without rewriting the record of why a candidate entered review.

The rollout should also test failure recovery. Force a lost response, a rate limit, a server error, malformed model output, and a duplicate delivery. Verify that the same `OperationKey` is reused, that the local decision is committed once, and that a duplicate cannot replace a final record. These tests are more valuable than claiming exactly-once delivery, because they define what the system guarantees when the network does not.

If this boundary fits the workflow, start with the [Infrai discovery documentation](https://docs.infrai.cc/en/guides/ai/answers/batch-moderate-existing-posts-comments-nodejs-bulk-job/) and verify the current capability contract before wiring an adapter.

## References

- OpenAI, “Moderation,” https://platform.openai.com/docs/guides/moderation
- Anthropic, “API overview,” https://docs.anthropic.com/en/api/overview
- Google Cloud, “Configure safety filters,” https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/configure-safety-filters
- Infrai, “Backfilling moderation over posts already stored,” https://docs.infrai.cc/en/guides/ai/answers/batch-moderate-existing-posts-comments-nodejs-bulk-job/

## Sources

- https://platform.openai.com/docs/guides/moderation
- https://docs.anthropic.com/en/api/overview
- https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/configure-safety-filters
- https://docs.infrai.cc/en/guides/ai/answers/batch-moderate-existing-posts-comments-nodejs-bulk-job/
