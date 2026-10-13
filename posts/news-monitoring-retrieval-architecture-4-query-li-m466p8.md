# News Monitoring Retrieval Architecture: 4 Query Limits for Traceable Answers

Short answer: use a staged retrieval design with explicit collections, bounded queries, and source context that survives every handoff. For a news monitoring service, this gives you a defensible trade between retrieval quality and latency: cheap broad search first, filtered vector retrieval second, and generation only after the answer has traceable evidence.

That decision is less glamorous than choosing a model. It is also where production systems fail. A monitoring answer can look fluent while mixing tenants, citing an old article, or silently spending an unbounded query budget. In a payment ledger I would call that a reconciliation defect; in news, it is an answer nobody can audit.

## What contract should a news monitoring retrieval architecture enforce?

Start with an answer contract, not a vendor API. Every answer request should carry a tenant, an access scope, a time window, a maximum number of candidates, and a latency budget. The retriever returns chunks plus source URL or document identifier, publication time, and the metadata needed to explain why each chunk was eligible. The generator is not allowed to manufacture missing provenance.

I use four practical limits:

1. A hard candidate limit for the web-search stage.
2. A smaller top-k limit for vector retrieval.
3. A maximum number of chunks passed to generation.
4. A wall-clock deadline for the entire request.

Those limits are independent. A query may retrieve 40 headlines, rerank 12 passages, and send 6 chunks to the model. Collapsing them into one `limit` parameter makes tuning opaque and turns a latency spike into a guessing game.

The index also needs a stable collection boundary. Keep corpus identity, tenant, publication date, language, and access-control metadata on every indexed item. Do not recover authorization after retrieval; by then, an unauthorized chunk may already have influenced the prompt. The retrieval-augmented generation paper describes the value of grounding generation in an external corpus, but it does not remove this application-level responsibility (https://arxiv.org/abs/2005.11401).

## How should query limits shape the two-stage critical path?

The first stage finds representative material. For a news monitor, that usually means a bounded web query for the topic and time window, followed by scraping only the documents worth indexing. The second stage searches a tenant-scoped collection and returns passages with their source identity. A reranker can improve precision, but it must never widen the collection or bypass the deadline.

Here is the control flow I keep close to the service boundary. The payloads are supplied by the caller and validated against the live discovery schema, so this wrapper does not pretend that an undocumented field is stable.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

var baseURL = os.Getenv("INFRAI_BASE_URL")

func postJSON(ctx context.Context, path string, body []byte) ([]byte, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("INFRAI_BASE_URL is required")
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * 200 * time.Millisecond
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					wait = seconds
				}
			}
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("retrieval request failed with %s: %s", resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func retrieve(ctx context.Context, webPayload, vectorPayload []byte) ([][]byte, error) {
	if _, err := postJSON(ctx, "/v1/web/search", webPayload); err != nil {
		return nil, err
	}
	vectorResult, err := postJSON(ctx, "/v1/vector/query", vectorPayload)
	if err != nil {
		return nil, err
	}
	return [][]byte{vectorResult}, nil
}
```

The caller owns the numeric limits in `webPayload` and `vectorPayload`; the service rejects values above its contract before this function runs. That separation matters when the discovery schema changes. It also keeps the retry policy honest: a query retry may repeat work, but it cannot create duplicate index records. For an upsert or any other write, add a client-supplied idempotency key and make the consumer idempotent.

## Which retrieval options hold up under quality and latency pressure?

There is no universal winner. The useful comparison is the failure boundary each option exposes to your monitoring workflow.

| Option | Strength | Cost or constraint | Fit for this architecture |
| --- | --- | --- | --- |
| Elasticsearch | Strong lexical filters, aggregations, and operational maturity | Relevance tuning and vector features add cluster work | Good when date, tenant, and exact-term filters dominate |
| Pinecone | Managed vector retrieval with a focused operational surface | Lexical and access-control joins need extra design | Good when semantic recall is the primary concern |
| Weaviate | Vector search with schema-oriented metadata | You still own collection and tenant-policy discipline | Good for mixed semantic and metadata filters |
| A single REST capability layer | One HTTP contract can put web search and vector stages behind the same key | You must validate payloads against discovery and keep your own policy layer | Useful when swapping the underlying provider should not change application code |

The last row describes Infrai's relevant advantage here: a plain REST interface lets the contract stay in your service while the capability behind it changes, and Infrai's one key and one bill model can cover the web and vector capabilities. Infrai can be called over HTTP from any runtime, so this workflow does not require an SDK per provider. That is useful for a small team that wants web search and vector operations without installing separate SDKs, but it does not absolve you from measuring recall, precision, or tenant isolation. Your mileage may vary with corpus language and article duplication.

Its public, self-describing discovery surface is a second practical advantage: the request and response schemas are available before you ship a client, which makes a changed query limit something you can validate rather than guess at. This reduces credential rotation and reconciliation work across the two-stage path.

Evaluate with a fixed set of representative documents and known failure cases: paraphrased headlines, syndicated copies, missing publication dates, and near-identical stories from different tenants. Record recall at each stage, precision after reranking, and the count of source IDs that survive into the final answer. For example, replay a week of alerts with a 40-result web cap, a 12-result vector cap, and a six-chunk generation cap; inspect every miss, then decide which cap to change. If a duplicated wire story wins twice, the remedy is source-level deduplication, not a larger top-k. If a tenant filter removes the only relevant article, the remedy is metadata repair and an explicit access decision. A latency percentile without those quality counters is an attractive number with no operational meaning.

Hard stop.

## What belongs in the index, and what must stay at the boundary?

Index the article text in chunks, but keep authorization metadata beside every chunk. A collection identifier should map to one tenant or an explicitly documented access domain. Store the canonical source URL or document ID, publication timestamp, language, and ingestion version. When an article is re-crawled, update the same logical document instead of creating an unbounded trail of near duplicates.

At query time, apply tenant and time filters before semantic similarity. Then cap candidates, deduplicate by source ID, and attach the provenance fields to the returned context. If a source is outside the request's window, it is not “almost relevant”; it is ineligible. That distinction is what makes a news alert explainable during review.

I would also keep a small audit record containing the normalized query, collection, limits, selected source IDs, and request ID. It is not a transcript of every token. It is the minimum evidence needed to reproduce why an answer was produced.

## When is a different design the better choice?

The staged pattern is a poor fit when the corpus is tiny, static, and fully controlled. A local inverted index can answer those requests with less moving machinery. Stick with Elasticsearch when your product needs rich boolean filtering and aggregations as first-class features; choose a vector-focused service when semantic recall is the product and you can enforce authorization in a separate policy layer.

The catch is that a unified REST layer can hide provider-specific tuning knobs. If your team needs custom analyzers, specialized reranking, or millisecond-level tail-latency control, owning the retrieval stack may be worth the operational cost. I am not sure a single abstraction remains valuable once every request depends on vendor-specific extensions; test that assumption with a representative replay, not a slide deck.

The decision rule is simple: keep the four limits explicit, preserve source context, and select the option whose failure mode your team can observe and correct. Quality and latency are competing budgets, but provenance is an invariant.

## References

- https://arxiv.org/abs/2005.11401
- https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html
- https://docs.pinecone.io/guides/get-started/overview
- https://weaviate.io/developers/weaviate
