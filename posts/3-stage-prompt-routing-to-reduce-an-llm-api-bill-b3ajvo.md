# 3-Stage Prompt Routing to Reduce an LLM API Bill for SaaS Moderation

A moderation report is not successfully classified merely because a model returned plausible prose. It succeeds only when the result conforms to the accepted schema, stays within its regional processing boundary, and can be reconciled to one durable decision before a human reviewer sees it. Short answer: use three stages: deterministic preprocessing, a small model for schema-bound classification, and a larger-model fallback triggered by validation or policy ambiguity. Batch only work whose review deadline permits it, and measure accepted decisions per input token rather than comparing headline prices.

This makes the expensive path narrow and inspectable. Every fallback carries a stable case identifier, a reason code, and the same normalized evidence; retries reuse an idempotency key. The accounting unit is an accepted, auditable classification, not an API call.

## How can a SaaS app reduce its LLM API bill?

A report-classification service may return only a category, severity, confidence band, queue, and bounded explanation. The hard part is refusing invalid combinations. A report cannot enter two mutually exclusive queues; a low-confidence decision may require escalation; and a region-constrained report must not cross its boundary merely because another runtime is available.

A small model earns the right to finish a case only after mechanical checks. Parse its response, reject unknown fields, enforce enumerations, check cross-field invariants, and verify that quoted evidence occurs in the normalized report. Do not ask another model whether the first emitted valid JSON when an ordinary validator can answer exactly. That adds expense and another probabilistic failure surface.

Stop there.

Retries compound.

Write the routing constraint before the code: accept a candidate only when syntax, schema, policy invariants, and regional rules pass; fall back once for an enumerated reason; send unresolved work to a human queue; and retain the request hash, policy version, model class, validation outcome, and disposition in an append-only trail. This is an exactly-once mindset, not a claim that networks deliver exactly once. Delivery may repeat. A conditional ledger write prevents repeated delivery from becoming repeated classification.

## Derive the router from correctness constraints

Begin with a canonical envelope. Preserve the original report separately, bind normalized content to a hash, and make region an execution constraint rather than prompt text. Deterministic preprocessing handles required metadata, size limits, exact duplicates, and approved redaction. The small model then produces one schema version. Only the validator can promote that candidate. A larger model sees a deliberately limited subset: malformed structure after the permitted retry, prohibited field combinations, policy-boundary ambiguity, or a category the smaller model is not authorized to finalize.

I would not use self-reported confidence as the only fallback signal. It is data, not proof. Calibrate thresholds against a labeled set, and keep structural failures separate from semantic disagreements; a single `confidence < 0.8` rule conceals why spend changed and weakens an audit reconstruction. The threshold itself belongs to application evaluation, not folklore.

This Go sketch leaves transport and vendors outside the decision core. The important property is `CommitOnce`: a timeout followed by redelivery cannot silently record a second final answer.

```go
package moderation

import "context"

type Case struct {
	ID, Region, PolicyVersion, ContentHash, Text string
}
type Decision struct {
	Category, Severity, ReviewQueue, EvidenceQuote string
}
type Candidate struct{ Decision Decision }

type Classifier interface {
	Classify(context.Context, Case) (Candidate, error)
}
type Validator interface {
	Check(Case, Candidate) (Decision, error)
}
type Ledger interface {
	CommitOnce(context.Context, string, Decision, Audit) (Decision, error)
}
type Audit struct {
	PolicyVersion, ContentHash, Route, FallbackReason string
}
type Router struct {
	Small, Large Classifier
	Validate     Validator
	Ledger       Ledger
}

func (r Router) Decide(ctx context.Context, c Case) (Decision, error) {
	candidate, err := r.Small.Classify(ctx, c)
	route, reason := "small", ""
	decision, invalid := r.Validate.Check(c, candidate)
	if err != nil || invalid != nil {
		route, reason = "large", "small_result_rejected"
		candidate, err = r.Large.Classify(ctx, c)
		if err != nil {
			return Decision{}, err
		}
		decision, err = r.Validate.Check(c, candidate)
		if err != nil {
			return Decision{}, err // caller sends the case to human review
		}
	}
	audit := Audit{c.PolicyVersion, c.ContentHash, route, reason}
	return r.Ledger.CommitOnce(ctx, c.ID, decision, audit)
}
```

Production code should distinguish transport failure, schema rejection, policy ambiguity, capacity failure, and regional ineligibility. That taxonomy decides whether retrying is safe, a larger model is useful, or the case must wait for a person. It also prevents a capacity event from converting every request into the costly path.

## Batch the queue, not the obligation

Batching changes scheduling, not correctness. Reports near a review deadline remain synchronous; eligible work with adequate slack can enter a regional queue keyed by policy and schema version. Mixing versions makes replay and reconciliation needlessly difficult.

Use durable transitions such as `received`, `submitted`, `validated`, `escalated`, and `committed`. A worker may execute twice, but only one transition to `committed` succeeds for a case key. Reconciliation compares submitted IDs with committed decisions and explicit exceptions. Missing IDs become visible.

Batch size follows measured limits and deadline slack, not a universal number. Large batches can reduce request overhead, but enlarge the retry unit and delay early results behind slow work. For moderation, a smaller retry unit can be worth the overhead because one malformed item should not obscure hundreds of valid decisions. This is a reliability trade-off.

Embeddings can identify related text, but similarity does not establish identical policy meaning. Use a similarity match as a routing signal or reviewer aid unless evaluation proves automatic reuse acceptable for that category. Record the embedding model identifier and threshold so the rule can be reconstructed. The cited embeddings guide describes embeddings as vector representations useful for relatedness tasks; it does not turn relatedness into policy equivalence.

## Measure the accepted decision

Compare routes on the same frozen report set, schema, policy version, regional boundary, and validator. Include ordinary cases, adversarial text, empty evidence, long inputs, supported languages, and examples close to category boundaries. Separate tuning data from final evaluation.

| Measure | Reason | Consequence |
| --- | --- | --- |
| Schema acceptance | Invalid structure cannot enter review | Reject before semantic fallback |
| Disagreement by category | Aggregate accuracy can hide risky classes | Narrow model authorization |
| Fallback by reason | Structure and ambiguity need different fixes | Repair the correct stage |
| Tokens per accepted decision | Retries are real consumption | Compare effective use |
| Deadline misses | Batch work can arrive too late | Keep low-slack work immediate |
| Duplicate commit attempts | Redelivery must be observable | Repair idempotency |
| Regional eligibility failures | Boundaries must survive fallback | Hold or use an eligible route |

Cost enters after acceptance. Count input and output tokens, retries, fallbacks, batch overhead, and human rework for each accepted decision. Keep commercial rates in configuration because they change. The ledger can then answer the durable question: which route satisfies the error budget and review deadline with the least measured resource use?

Do not optimize from aggregate fallback rate alone. A route that handles easy reports but fails disproportionately on severe categories may appear economical while delaying the riskiest cases. Break results down by policy category, language, length band, region, and reason, subject to the retention and access limits governing moderation data.

## Roll out in 3 reversible steps

First, shadow the small route on a representative slice while the established path remains authoritative. Compare both through the same validator and adjudicated samples. Second, enable small-first routing for one policy version and bounded traffic, with a kill switch that changes routing without changing case identity or ledger shape. Finally, admit eligible low-urgency cases to regional batch queues and reconcile every submitted ID to a decision or explicit exception before expanding.

**The durable design is a constrained decision pipeline: deterministic work first, the least capable authorized model next, one validated fallback, and a human boundary that the runtime cannot spend its way around.**

## Sources

- https://platform.openai.com/docs/guides/embeddings
