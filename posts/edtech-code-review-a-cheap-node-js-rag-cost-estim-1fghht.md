# Edtech Code Review: A Cheap Node.js RAG Cost Estimate with Token-Bounded Search

Short answer: a cheap RAG cost estimate for Node.js should count document tokens, batch embeddings once, and bound semantic-search context before an LLM reviews an edtech change. Make the retrieval contract and token ledger provider-neutral, then return findings through a versioned schema; that is a better cost control than treating a low per-request price as the architecture.

The concrete workload matters. An edtech team wants a reviewer to answer questions about its own repositories and return structured findings on a proposed change: file, line, severity, rule, evidence, and confidence. Semantic search is useful for finding similar patterns in design notes, test guidance, and prior review decisions, but the answer is only as trustworthy as the evidence lineage behind it. A repository snapshot, chunk, retrieval result, and review request therefore need durable identities. In a ledger backend I would never reconcile a debit using a mutable description; the same discipline applies here.

The first failure is usually not the model. It is an index that cannot explain which source version produced a result. The second is a retry that writes duplicate chunks. The third is a prompt that grows because every retrieved passage is considered equally important. Each failure can raise spend and make a finding harder to audit at the same time.

This is a ledger problem.

For example, suppose a pull request changes an authentication helper and the reviewer retrieves four passages: a current secure-coding rule, an old migration note, a neighboring test, and a tenant's unrelated style guide. A token-only estimate sees four chunks and stops there. A useful estimate records the candidate set, the metadata filters, the final evidence set, and the rejected candidates, because each stage changes both the review result and the amount of context sent to generation. The ledger can then answer whether a cost increase came from more changed documents, a larger chunk overlap, a new retrieval limit, or a prompt template that began repeating the diff. It can also explain a missed finding: perhaps the relevant rule was indexed under a prior revision and correctly excluded, or perhaps the filter was too strict. Those are different engineering decisions with different remedies. Keeping them in one mutable counter erases the distinction, while a small event record preserves it without requiring the model provider to understand the repository's business rules.

## How can a Node.js RAG design keep token counts visible while semantic search stays portable?

Start with two records: an ingestion estimate for changed documents and a query estimate for each review. The ingestion side includes the source version, chunker version, overlap, embedding input tokens, and batch identity. The query side includes instruction tokens, the diff, the question, retrieved context, output tokens, and the model-independent review schema. Do not infer these values from character count once the system is near a budget boundary; sample the actual corpus with the tokenizer selected for the deployment.

The arithmetic is intentionally boring. That is a feature. A forecast should be reproducible by another engineer six months later, after a chunker change or a provider migration.

```go
package main

import "fmt"

type ReviewPlan struct {
	ChangedDocuments   int64
	ChunksPerDocument  int64
	TokensPerChunk     int64
	MonthlyReviews     int64
	RetrievedChunks    int64
	InstructionTokens  int64
	DiffTokens         int64
	QuestionTokens     int64
	FindingTokens      int64
}

func main() {
	plan := ReviewPlan{
		ChangedDocuments:  240,
		ChunksPerDocument: 6,
		TokensPerChunk:    280,
		MonthlyReviews:    1800,
		RetrievedChunks:   4,
		InstructionTokens: 180,
		DiffTokens:        420,
		QuestionTokens:    32,
		FindingTokens:     260,
	}

	indexTokens := plan.ChangedDocuments * plan.ChunksPerDocument * plan.TokensPerChunk
	contextTokens := plan.RetrievedChunks * plan.TokensPerChunk
	reviewInput := plan.InstructionTokens + plan.DiffTokens + plan.QuestionTokens + contextTokens
	reviewInputTotal := plan.MonthlyReviews * reviewInput
	reviewOutputTotal := plan.MonthlyReviews * plan.FindingTokens

	fmt.Printf("index input tokens: %d\n", indexTokens)
	fmt.Printf("monthly review input tokens: %d\n", reviewInputTotal)
	fmt.Printf("monthly structured output tokens: %d\n", reviewOutputTotal)
}
```

Those numbers are planning inputs, not a promised invoice. Store the corpus sample, tokenizer identifier, chunker version, retrieval limit, and schema version with the estimate. If the monthly input total rises, the team can distinguish more reviews from a wider context window. This is also where provider portability becomes real: an adapter can translate a common `Embed`, `Search`, and `GenerateReview` interface, while the application retains the same accounting fields and acceptance tests.

## The durable unit is a source version, not a request

Batching document indexing is valuable because it creates a bounded unit for scheduling and reconciliation. It is not a magic discount. A batch should contain a deterministic operation ID, source-version IDs, a content digest, the embedding configuration, and the intended vector keys. A worker may retry transport; the resulting effect must remain one logical index version.

Consider a review-policy document edited while a batch is in flight. If the worker keys only on the path, the old and new chunks can both be called `review-policy-3`, and a late response can overwrite the newer evidence without any obvious exception. If it keys on tenant, content digest, revision, chunk number, and chunker version, the two results remain distinct; an activation record can then point to exactly one complete manifest. The same record lets an operator answer which policy text supported a finding, which embedding configuration produced its vector, whether the batch was retried, and which review request consumed it. That is a longer write path, but it is cheaper than reconstructing an audit trail from logs after a disputed code-review result.

Use an atomic upsert keyed by `(tenant, source_version, chunk_number, chunker_version)`. Mark the source version active only after every expected chunk has a recorded result and the vector store contains the corresponding keys. Keep the previous active version until the new one passes a retrieval evaluation. Deleting first creates a gap in which a reviewer can receive no evidence, while overwriting in place makes rollback and audit reconstruction needlessly difficult.

The review request needs the same treatment. Give it a request ID, diff digest, repository revision, retrieval configuration, and an evidence list. A repeated delivery can then return the prior structured result instead of charging for a second logical review. Exactly-once delivery is not a sensible network assumption; exactly-once effect is an application property.

Short version: retries are normal. Duplicate business effects are not.

## Retrieval quality is a cost control with a compliance boundary

For code review, nearest-neighbor similarity is a candidate generator, not a finding. A passage may be semantically close while belonging to an old repository revision, another tenant, or a different language rule. Filter by tenant and revision before ranking, retain the chunk IDs used by the answer, and make the reviewer cite those IDs in its structured output. A human can then inspect why a finding was returned instead of accepting an opaque score.

The context budget should be tested against review quality. Retrieve a wider candidate set, apply deterministic metadata filters and an optional reranker, then send only the evidence that the evaluation set shows to be useful. The right number is not universal. A smaller context can lower repeated input work, but an aggressive limit can hide the policy paragraph that explains a security finding. Measure false positives, missed findings, latency, input tokens, and output tokens together.

The schema is part of the control plane. A function-calling style contract can require an array of findings with typed severity, a source location, a rule ID, and evidence references; validation must reject an answer that cites a chunk not present in the request. The public function-calling guide describes the general mechanism, but the application still owns authorization, schema validation, and retention decisions.

For student records or other regulated data, retrieval quality does not replace access control or minimum-necessary handling. 45 CFR Part 164 provides the security and privacy rule text for covered HIPAA workloads; it does not certify a particular retrieval design or provider. Log authorization decisions and evidence references, redact fields before indexing where policy requires it, and define who may read the review trace.

## Provider portability needs a testable contract

Portability is more than swapping a base URL. Embedding dimensions, distance metrics, maximum input sizes, batching semantics, metadata filters, rate-limit behavior, tool-call syntax, retention options, and regional availability can differ. Hide those differences behind an adapter, but do not hide them from the test suite.

| Integration shape | Best fit | Main trade-off |
| --- | --- | --- |
| Direct REST adapter | A team that wants its own request IDs, persistence, and retries | The application owns timeout, backoff, schema validation, and observability |
| Hosted gateway | A team that needs one boundary while comparing several backends | Gateway-specific limits and metadata translation must be tested |
| Self-hosted components | A team with a strong operations group and a fixed region requirement | Capacity planning, model updates, and vector maintenance stay in-house |

The table is a decision frame, not a ranking. The correct row is the one whose controls can produce the evidence the review process requires.

The contract should include four checks. First, the same corpus fixture must produce an index manifest that records model and dimension metadata. Second, a fixed question set must meet a relevance threshold before a provider is promoted. Third, malformed structured output must be rejected without persisting a finding. Fourth, a retry of the same operation must leave one active source version and one review result. These tests expose the real migration cost without turning the article into a vendor comparison.

A plain HTTP boundary is often the least coupled integration for a Node.js service: the application can own request IDs, timeouts, and persistence, and a separate worker in another language can implement the same adapter contract. The trade-off is that the team must implement and observe those controls itself. A hosted abstraction is not suitable when a workload requires a capability, region, retention policy, or evidence format that the abstraction does not expose; stick with a directly controlled provider in that case.

I'm not sure any forecast remains accurate after a repository changes its documentation style, so I would schedule a monthly replay of the evaluation corpus and compare distributions rather than trusting one average. Your mileage may vary with code density and review size. The important part is that the uncertainty is visible and tied to a versioned input set.

## A small rollout that can be reconciled

Freeze a representative set of repositories, diffs, questions, and expected findings. Assign immutable IDs, measure token distributions, and compare chunk sizes and retrieval limits before enabling generation. Keep a small canary tenant isolated from the production index, and require a passing relevance and schema-validation report before promotion.

During the canary, record one row per source version, batch, embedding operation, retrieval decision, and review result. Alert on duplicate operation IDs, unexpected context growth, missing evidence references, and changes in the structured-finding distribution. Do not use an aggregate bill as the primary alarm; by the time it moves, the causal configuration may already be gone.

The least complex design that preserves those records is usually the right first release. It can be replaced later, because the contract, corpus IDs, and evaluation set remain yours.

## Further reading

- OpenAI, Function Calling guide: https://platform.openai.com/docs/guides/function-calling
- Electronic Code of Federal Regulations, 45 CFR Part 164: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164
