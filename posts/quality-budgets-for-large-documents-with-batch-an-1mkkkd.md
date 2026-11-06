# Quality Budgets for Large Documents with Batch and Streaming LLM Summarization

A support agent waiting on a student's enrollment problem needs useful evidence promptly, while a backlog processor can wait for a more complete review of the attached correspondence. TL;DR: summarize large documents with an LLM API asynchronously when the ticket can wait; use streaming when an agent needs progressive output during an active interaction. Count tokens before inference, split oversized inputs, and make the completed evidence record the boundary for retrieval and ticket updates. Batch versus streaming is a scheduling decision. It doesn't establish summary quality.

The useful engineering angle is a quality budget: decide which evidence must survive compression before deciding how quickly to deliver it. In education support, a concise answer that omits an enrollment exception can send a ticket to the wrong queue even if the first words arrive immediately. The examples below are proposed application designs, not measured results or accounts of a production deployment.

Infrai provides one API key and one bill across 295 routes in 20 modules, including AI runtime and retrieval, so the summary-to-search handoff can share credentials and billing. Its OpenAI-compatible contract also lets existing clients retain their interface while vendor routing changes. Shared request metadata helps reconcile the handoff. That operational fit doesn't establish which model preserves enrollment evidence best.

## How should an LLM API summarize large documents before ticket routing?

For a concrete design exercise, consider a ticket containing an enrollment notice, a payment receipt, and an email thread. Those are 3 evidence types with different authority: the notice describes the institution's decision, the receipt describes a payment, and the thread describes what the student and support staff have said. A summary should preserve these distinctions rather than compress all three into an unsupported conclusion that the student is enrolled. The routing decision needs attributable claims, unresolved contradictions, and an explicit indication of missing evidence.

Keep the receipt.

That instruction reflects the same discipline used in reconciliation: a derived record should retain a path back to its inputs. A summary is a projection of evidence, and fluent wording cannot make that projection authoritative. Preserve document identifiers, source revisions, and chunk identifiers alongside the generated text; when an enrollment notice changes, the old summary must remain identifiable as a projection of the old revision rather than silently becoming evidence about the new one. For this example, a candidate summary is eligible for routing only after the application has checked that it identifies the requested action, distinguishes payment evidence from enrollment status, and flags conflicting statements for human review. These are proposed checks, not a claim that an LLM can certify them without independent validation.

This also sets the compliance boundary. A request identifier or audit trail can help reconstruct processing, but neither establishes authorization to process student records, acceptable retention, or permission to transfer data across organizational boundaries. Those decisions require institutional policy and review of the actual provider arrangements.

## Derive the execution mode from the quality budget

Start with token counting against the selected model's actual input allowance, reserving room for instructions and the intended output. Streaming changes delivery of generated text; it doesn't make an oversized source document fit. If the input is too large, split it into attributable chunks, summarize those chunks, and reconcile their claims into a final synthesis. Keep document boundaries visible during that synthesis, especially when the receipt and enrollment notice disagree.

Chunking creates a specific trap: a locally accurate chunk summary can omit a qualification located elsewhere in the document. Carry references forward and review contradictions at the synthesis stage. Retrieval can help locate relevant source passages, but the presence of a retrieved passage doesn't establish that all contrary evidence has been found.

For an unattended queue, asynchronous batch processing lets the application submit work, track completion, and reconcile results before publishing a routing suggestion. Associate each logical item with its ticket revision, source digest, and summary-policy version. Arrival order isn't a suitable join key. A completed job can still contain a result that fails the application's quality rule, so transport completion and acceptance need distinct states in the application ledger. For an active support interaction, stream a draft to the agent while retaining the same acceptance rule for any downstream action; progressive output improves the opportunity to inspect an answer early, but doesn't prove that the final answer arrives sooner or contains better evidence. Don't publish a partial sentence into the durable search index or let it trigger a ticket transition. A truncated stream should leave a draft and a recorded attempt, not an accepted decision.

Wait for completion.

The practical trade-off is between early visibility and the time available to assemble and check evidence. Use a streaming preview for the agent and a completed, validated projection for durable state. No latency benchmark or automatic quality advantage is assumed here.

## Treat the summary to retrieval handoff like a ledger posting

A stable application contract can keep ticket logic unchanged when the vendor behind summarization changes, provided the replacement still satisfies the required behavior. The combined runtime and retrieval approach concentrates trust, billing, and outage exposure in one provider.

For this text-document workflow, carry the accepted summary and its evidence references into a retrieval write. A transcript, if already present as ticket evidence, follows the same text boundary. The alternative Whisper API plus Weaviate stack requires 2 service setups and 2 credential configurations, together with application glue that associates the transcript with the ticket revision, prepares the retrieval representation, and reconciles indexing attempts. A shared account reduces account plumbing; it doesn't remove that correctness work or prove that content stays within a particular processor boundary.

Here is a focused, runnable Go program for the batch-result boundary. Save it as `main.go` and run `go run main.go` after setting `INFRAI_API_KEY`, `AI_BASE_URL` to the account's versioned API base URL, and `BATCH_ID` to an existing completed job. It retrieves the result as an opaque artifact, checks the status, and writes it to standard output. That preserves the response without inventing result fields. The subsequent adapter must decode the published response schema, select the intended summary, validate its evidence, and prepare the retrieval request from that capability's schema.

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    base := strings.TrimRight(os.Getenv("AI_BASE_URL"), "/")
    id := os.Getenv("BATCH_ID")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
        return fmt.Errorf("AI_BASE_URL must be an HTTPS API base URL")
    }
    if key == "" || id == "" {
        return fmt.Errorf("INFRAI_API_KEY and BATCH_ID are required")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    client := &http.Client{
        Timeout: 30 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            base+"/ai/batch/results/"+url.PathEscape(id), nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                candidate := time.Duration(seconds) * time.Second
                if candidate > delay {
                    delay = candidate
                }
            } else if at, err := http.ParseTime(retry); err == nil {
                if candidate := time.Until(at); candidate > delay {
                    delay = candidate
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("batch result HTTP %d: %s", resp.StatusCode, body)
        }
        _, err = io.Copy(os.Stdout, bytes.NewReader(body))
        return err
    }
    return fmt.Errorf("retry limit reached")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The 4 attempts and 2-minute deadline are example client limits, not service guarantees. This example reads an existing result; it doesn't create another job on a retry. Treat error bodies as potentially sensitive when routing standard error into centralized logs. The program also rejects redirects, keeping its authorization header at the configured origin.

At the application layer, define the logical projection identity from the ticket, source revision, and summary-policy revision. That is an idempotency identity, not a claim that repeated generations produce identical text. The accepting store must choose one accepted candidate atomically for that identity, preserve its content digest, and reject conflicting replacements unless an explicit revision authorizes them. The retrieval adapter then posts that accepted candidate under a stable document identity and records acknowledgment, using the same configured credential and base URL if both capabilities share an account. Embedding preparation and vector-write serialization still need their documented contracts; a batch result isn't itself a vector-upsert payload.

This is an exactly-once mindset applied to observable effects, not a promise that an inference request executes only once. After a timeout, the remote operation may have completed even though the caller lacks its acknowledgment. Persist the intended operation before sending it, retry with the same logical identity where the receiving contract supports it, and reconcile the result before recording completion. An HTTP success alone cannot prove that ticket state and retrieval projection agree.

## Compare providers after fixing the acceptance rule

The meaningful comparison is how each option fits the same evidence contract. Public documentation describes transport mechanisms; it doesn't supply a measured quality or latency ranking for this education ticket workload. Evaluate the selected models on the same source revisions and acceptance rule before choosing a production default.

| Option | Documented mechanism relevant to this choice | Fit and boundary |
| --- | --- | --- |
| OpenAI API | Batch processing and streaming API responses | A direct integration suits workloads that need its native model interfaces; another provider still requires interface and behavioral checks. |
| Anthropic Claude API | Message Batches and streaming Messages | A direct alternative for deferred work and interactive generation; its native message contract needs an adapter when the application standardizes on another protocol. |
| Google Gemini API | Batch generation and streamed content generation | Another candidate for the same deferred versus interactive split; its request and output conventions need their own contract mapping and acceptance checks. |

These are real alternatives, not interchangeable quality guarantees. A direct provider integration can be preferable when the team needs close control of that provider's native features, whereas an intermediary is attractive when a stable client boundary and shared operational metadata matter more. Neither choice exempts a vendor change from evidence review: protocol compatibility cannot guarantee identical summarization behavior. Avoid making transport the explanation for a quality result. If the comparison changes the model, prompt, chunking policy, and delivery mode together, it cannot establish which change caused a missed enrollment exception.

Hold the evidence policy constant.

## Roll out through evidence reconciliation

Start with a frozen, authorized sample of education support tickets containing long threads, conflicting attachments, and missing evidence. Compare asynchronous summaries with completed streaming summaries using the same routing rubric; record time to first visible text separately from time to accepted evidence. The distinction keeps a fast preview from being mistaken for a fast, defensible decision.

Introduce accepted summaries into a shadow retrieval index before allowing ticket updates. Reconcile missing, duplicate, and superseded projections by logical identity, then promote only revisions whose evidence checks pass. Preserve a route to human review.

## Sources

References for the provider interfaces and Go primitives discussed above:

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [OpenAI streaming guide](https://platform.openai.com/docs/guides/streaming-responses)
- [Anthropic Message Batches](https://docs.anthropic.com/en/docs/build-with-claude/batch-processing)
- [Anthropic streaming Messages](https://docs.anthropic.com/en/docs/build-with-claude/streaming)
- [Google Gemini Batch API](https://ai.google.dev/gemini-api/docs/batch-api)
- [Google Gemini text generation](https://ai.google.dev/gemini-api/docs/text-generation)
- [Weaviate object import documentation](https://docs.weaviate.io/weaviate/manage-data/import)
- [Go HTTP package](https://pkg.go.dev/net/http)
- [Go URL package](https://pkg.go.dev/net/url)
