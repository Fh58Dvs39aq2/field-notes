# Node.js LLM Structured Findings: Retry-Proof JSON Records

Use a durable review-attempt record, a deterministic finding key, and one database transaction to make a Node.js LLM JSON pipeline converge after retries. The deciding constraint is structured-output correctness: a webhook delivery may be repeated, but a code-review finding must not be inserted twice or silently replaced by a later, incompatible extraction.

This is an architecture decision record for an e-commerce repository that asks an LLM to inspect a code change and return findings such as severity, file, line, rule, and explanation. The network remains at-least-once. The useful guarantee is exactly-once effect at the commit boundary, with an audit trail that makes reconciliation possible.

## The decision: separate the review from every delivery attempt

The business identity is the review request, not the webhook event ID and not the model request ID. Assign an opaque `review_id` when the change enters the system, persist the immutable diff digest and prompt/schema version, and give every provider call an `attempt_id`. A webhook `event_id` is useful for deduplicating notifications, but it cannot identify the business result: a sender can emit two events for one attempt, and a retry may have a new event ID.

For each finding, derive a stable key from the review ID, schema version, file path, line or range, rule ID, and a normalized finding fingerprint. The exact fields should reflect the product's merge-review semantics. A unique database constraint must enforce the chosen key; a JavaScript `Set` in the worker is only an optimization for one process and one lifetime.

The invariant is narrow and testable:

1. One review may have many attempts, but only the selected schema version may materialize findings.
2. Replaying an already committed attempt produces an audited no-op.
3. A validation failure stores the raw result in quarantine and creates no finding.
4. A database failure retries the transaction from durable result data; it does not call the model again merely because the commit outcome is unknown.

That last boundary matters. A worker can lose its connection after the database commits and before it acknowledges the webhook. The next delivery must read durable state, not trust an in-memory “failed” flag.

| Option | Preserves | Failure it leaves to the application |
| --- | --- | --- |
| Event ID as the primary key | Notification deduplication | It does not prevent two events for one review from creating duplicate findings |
| Model request ID as the primary key | Provider-attempt deduplication | It does not express which review and schema version own the side effect |
| Review ID plus a finding key | Business-level convergence | The application must define normalization and enforce uniqueness in the database |
| Content hash alone | Convenient repeat detection | Two legitimate revisions with identical text can be conflated without a review identity |

The review ID plus finding key is the selected boundary. It is less clever than letting the queue decide, which is exactly why it survives queue changes.

## How should LLM extraction retries, webhook workers, and duplicate JSON records interact?

Make the worker a state transition processor, not a second submission engine. On notification, authenticate the message, record the event ID, load the attempt, and inspect its durable state. If the attempt is still pending, schedule a poll or retry according to its lease. If a result is already present, validate and materialize that result. If the review is committed, record the redelivery and return success without inserting another finding.

The retry policy should distinguish failure domains. A timeout while waiting for a model response is an unknown observation, not proof that the model did not receive the request. Retry status observation first. A malformed JSON result is a data-quality outcome: quarantine it with the schema version and validation errors, then route it for review. A serialization or database timeout is a write uncertainty: replay the same durable payload under the same uniqueness constraints. Only an explicitly retryable model request should create another attempt, and that new attempt must remain linked to the same `review_id`.

For payment and ledger-adjacent workflows, I record `review_id`, `attempt_id`, result digest, schema version, transition, event ID, correlation ID, and timestamps. The audit row answers “what did we observe?”; the unique index answers “what side effect may exist?” Keep customer text and source code out of ordinary logs when retention or access rules make that unsafe. A digest is an integrity check, not a reversible substitute for access control.

Short rule: the database decides.

The critical path below uses Go to make the transaction boundary explicit. In production, `Store` represents a real transaction with unique indexes and an outbox or audit table; the example deliberately keeps the domain logic visible instead of hiding it in a queue helper.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type Finding struct {
	Key      string          `json:"key"`
	Severity string          `json:"severity"`
	File     string          `json:"file"`
	Line     int             `json:"line"`
	Rule     string          `json:"rule"`
	Text     string          `json:"text"`
}

type ReviewStore struct {
	Findings map[string]Finding
	Attempts map[string]string
	Audit    []string
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Materialize is the work a database transaction must make atomic.
func (s *ReviewStore) Materialize(reviewID, attemptID, schema string, raw []byte) error {
	var findings []Finding
	if err := json.Unmarshal(raw, &findings); err != nil {
		s.Audit = append(s.Audit, fmt.Sprintf("quarantined review=%s schema=%s", reviewID, schema))
		return err
	}

	if prior, exists := s.Attempts[attemptID]; exists {
		s.Audit = append(s.Audit, fmt.Sprintf("redelivery attempt=%s review=%s", attemptID, prior))
		return nil
	}

	for _, finding := range findings {
		if _, exists := s.Findings[finding.Key]; exists {
			s.Audit = append(s.Audit, fmt.Sprintf("duplicate finding=%s review=%s", finding.Key, reviewID))
			continue
		}
		s.Findings[finding.Key] = finding
	}
	s.Attempts[attemptID] = reviewID
	s.Audit = append(s.Audit, fmt.Sprintf("materialized review=%s digest=%s", reviewID, digest(raw)))
	return nil
}

func main() {
	store := ReviewStore{Findings: map[string]Finding{}, Attempts: map[string]string{}}
	result := []byte(`[{"key":"rev-42|schema-3|cart.go|87|currency-check","severity":"high","file":"cart.go","line":87,"rule":"currency-check","text":"Currency is not validated before total calculation."}]`)
	_ = store.Materialize("rev-42", "attempt-7", "schema-3", result)
	_ = store.Materialize("rev-42", "attempt-7", "schema-3", result)
	fmt.Printf("findings=%d audit_events=%d\n", len(store.Findings), len(store.Audit))
}
```

The map is not concurrency protection. The real implementation needs a transaction and a unique index, plus an outbox if downstream publication must be coupled to the commit. A test should crash or simulate an acknowledgement loss at each boundary: before validation, after result storage, after finding insertion, and after commit. The expected result is one finding, a replayable audit record, and no second model submission caused by an ambiguous write.

## What should a structured JSON review pipeline validate before commit?

Treat the model response as untrusted input. Parse it, validate the schema, reject unknown or unsafe fields according to the contract, and normalize only fields whose semantics are documented. Severity should come from a finite set; line numbers should be non-negative and tied to the submitted revision; file paths should not escape the repository scope; and the explanation should be bounded for storage and rendering. A valid JSON document can still describe the wrong revision, so bind the result to the stored diff digest and schema version before materialization.

Prompt instructions are not a security boundary. A changed file can contain text that attempts to redirect the model, and the resulting prose can contain misleading instructions for a later agent. Keep the requested output schema small, treat findings as data, and run authorization, repository-scope, and policy checks outside the model. OWASP's LLM guidance is useful here because the failure is not limited to parsing: prompt injection, insecure output handling, and sensitive-information disclosure can all occur around a syntactically perfect response.

I would also retain the original result digest and a redacted validation report. Reprocessing needs the original evidence, while ordinary observability needs the least sensitive representation. The audit trail is part of the product for an e-commerce team: when a release is blocked by a finding, an operator must be able to explain which revision, contract, and attempt produced it.

## Rejected design and its valid use case

The rejected design is “submit on every webhook retry, then delete duplicates later.” It turns an uncertain observation into a new side effect, makes provider usage and audit history ambiguous, and can delete two legitimate findings that happen to share a weak fingerprint. Cleanup is not idempotency.

That design has a valid use case for a side-effect-free experiment whose output is intentionally discarded, such as comparing prompts against a fixed offline corpus. It is not suitable for a review worker that gates a merge or creates a compliance record. Once a result affects a release decision, retry identity must be durable before the first external call.

The catch is operational ownership. A relational database is a strong fit when the team already relies on unique constraints and transactions; an event store or document database can work when it provides an equivalent atomic conditional write and a clear audit query. Your mileage may vary on transaction syntax, but the invariant does not: delivery retries must converge at the business write boundary.

## References

- OWASP Top 10 for Large Language Model Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- Prompt Engineering Guide: https://www.promptingguide.ai
