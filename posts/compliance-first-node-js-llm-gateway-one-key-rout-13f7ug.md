# Compliance-first Node.js LLM gateway: one-key routing across OpenAI, Claude, and Gemini

Short answer: a unified LLM API is a sensible first boundary for a Node.js backend that needs text or structured output from OpenAI, Claude, and Gemini, provided that the application keeps the model decision, region evidence, and retry ledger under its own control.

The attractive property is mundane and useful: one backend key and one HTTP contract can sit behind a small internal adapter. That reduces credential distribution and SDK churn. It does not make providers interchangeable in every respect, and it does not turn an inference request into an exactly-once business transaction.

This is an architecture decision record for a US/EU service. The invariants are idempotency, an append-only audit trail, explicit model availability, and a cost estimate that remains an estimate. Compliance review still decides which prompts may cross a regional or processor boundary.

## What should a Node.js backend record when one key routes OpenAI, Claude, and Gemini?

Record more than the response. For each logical operation, persist a client-generated operation ID, the prompt or template version, the selected model ID, the policy version, the region decision, every transport attempt, the final status, and a digest of the returned content. A retry must point to the same logical operation. Otherwise a timeout followed by a successful retry looks like two billable events in a ledger, even when the business action should advance once.

The gateway is a transport boundary; the application is the system of record. That distinction is the useful correction to the phrase “exactly once.” HTTP can deliver a response after the caller has timed out, or the caller can retry after the provider has completed generation. Idempotency keys and reconciliation make the surrounding operation effectively once-only; a vendor cannot infer the business invariant for you.

Discovery comes before routing. Query the model catalog, retain only entries marked available, and map their IDs into a versioned policy. A policy can say that a payment explanation may use an approved text model in the US or EU, while a regulated record requires a narrower set. If no approved model is available, fail closed and leave the business record pending for review. Do not silently substitute a model whose processing terms have not been assessed. The catalog is an input to policy, not the policy itself.

Make that decision visible.

No magic.

Consider a payment explanation that is generated just before a ledger entry is posted. The first request may reach a provider, but the response can be lost between the gateway and the worker; a second attempt may then return a perfectly valid answer. The ledger must therefore treat the operation ID as the identity of the explanation, not the identity of a network attempt, and it must retain both attempts beneath that parent record. A reconciliation job can compare the final response digest, usage metadata, and policy version before permitting the downstream entry. If the records disagree, the safe outcome is a pending review, not a second financial mutation. This is slower than pretending a single HTTP response is a transaction, but it is legible to an auditor and testable in code.

Cost estimation belongs in the same decision record but not in the settlement column. Use it to reject an oversized prompt or choose between approved models, then reconcile the completed call against the usage data your integration receives. Estimated and actual values are different facts. Mixing them makes an audit trail hard to defend.

## Where does a unified gateway fit, and where does it stop?

The managed REST option fits when the first release is text-first and the team wants a single integration surface. Infrai exposes a plain HTTP API, so a Node.js service, a Go worker, or another HTTP-capable runtime can call it without installing an SDK or tracking a client-library version. Its chat-compatible endpoint, model discovery, and cost-estimation capability align with the decision path above.

The limits are material. Realtime voice sessions are pending and region-limited to the western region, so this is not a production voice router. The catalog also reports ASR as unavailable. There is no dedicated moderation endpoint; a team that needs moderation must use a chat model with a JSON Schema response as a fallback and should validate that design separately. Batch APIs can be added later for offline prompts, rather than burdening the first synchronous path with a second execution model.

That is a capability boundary, not a defect. Choose a specialist service when voice, production transcription, or a dedicated moderation control is a hard requirement. Stick with a direct provider when its regional contract or provider-specific feature is the requirement. Your mileage may vary because those organizational constraints are not visible in an API catalog.

## Comparing direct providers, a self-hosted proxy, and a managed REST layer

There is no universal winner. The comparison is about who owns credentials, routing policy, and operational work.

| Option | Key and integration | Audit and routing effect | Good fit | Trade-off |
|---|---|---|---|---|
| Direct OpenAI | Provider-specific key and client | Application maps one provider's IDs and errors | OpenAI-specific controls are decisive | Multi-provider switching remains application work |
| Direct Anthropic Claude | Provider-specific key and client | Application owns Claude-specific attribution | Claude is the deliberate primary dependency | A second provider needs another adapter and secret |
| Direct Google Gemini | Provider-specific key and client | Application owns Gemini-specific attribution | Gemini-specific behavior is central | Cross-provider policy is assembled by the team |
| LiteLLM | Self-hosted gateway and its credentials | Team owns proxy deployment, logs, and policy | Infrastructure ownership is a requirement | Running and securing the proxy is part of the service |
| Managed REST gateway | One backend key and an HTTP adapter | Discovery can inform routing; application keeps the ledger | Text/chat across vendors with minimal client machinery | Unsupported modalities and regional limits still require another service |

For a small backend team, the managed row is defensible when the workload is text or structured output and the organization accepts its stated boundaries. A platform team already staffed to run shared infrastructure may prefer LiteLLM because operating the gateway is an intentional control, not accidental overhead. Direct integrations remain the right answer when a provider-specific API, contract, or data-processing term cannot be abstracted.

The rejected option for the first release is three simultaneous vendor SDKs wired directly into business handlers. That design looks explicit, but it duplicates credential rotation, error normalization, model selection, and audit mapping before the team knows which provider-specific features it needs. It becomes a valid later choice for a workload whose differentiating feature exists only in one vendor's native API.

## A minimal critical path in Go

The production service may be Node.js; the example uses Go to make the HTTP contract and retry boundary unambiguous. It discovers an available model, then submits one chat request. The key comes from the environment, methods are explicit, and a client operation ID is reused for write retries. The response body is surfaced on non-success status codes. Cost estimation remains a separate policy call in the service, recorded before routing without making the code sample a catalogue of endpoints.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type model struct {
	ID        string `json:"id"`
	Available bool   `json:"available"`
}

type modelList struct {
	Data []model `json:"data"`
}

func request(ctx context.Context, client *http.Client, method, url, key, operationID string, payload []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payload))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")
		if len(payload) > 0 {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", operationID)
		}
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed (%d): %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate-limit retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	base := os.Getenv("LLM_API_BASE_URL")
	if key == "" || base == "" { panic("INFRAI_API_KEY and LLM_API_BASE_URL are required") }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 25 * time.Second}
	operationID := "payment-explanation-2026-0001" // generated and stored by the caller in production

	raw, err := request(ctx, client, http.MethodGet, base+"/v1/models", key, operationID, nil)
	if err != nil { panic(err) }
	var catalog modelList
	if err := json.Unmarshal(raw, &catalog); err != nil { panic(err) }
	chosen := ""
	for _, candidate := range catalog.Data {
		if candidate.Available { chosen = candidate.ID; break }
	}
	if chosen == "" { panic("no available model") }

	payload, err := json.Marshal(map[string]any{
		"model": chosen,
		"messages": []map[string]string{{"role": "user", "content": "Return a JSON risk summary."}},
	})
	if err != nil { panic(err) }
	result, err := request(ctx, client, http.MethodPost, base+"/v1/chat/completions", key, operationID, payload)
	if err != nil { panic(err) }
	fmt.Println(string(result))
}
```

The operation ID in this sample is a placeholder value for a caller-owned identifier; production code should generate it, persist it before the first attempt, and use the same value when reconciling a timeout. The catalog response shape and cost-estimate fields should be validated against the current service contract before shipping. I am not sure that a generic model ID can carry every provider's safety or retention nuance, which is another reason to retain those attributes in your own policy table.

## Review triggers and the decision boundary

Approve this design for synchronous text and structured-output traffic when one credential surface, provider choice at policy time, and an application-owned audit trail are the priorities. Revisit it before adding realtime voice, ASR, or a separate moderation product. Revisit it as well when batch workloads become large enough to justify their own queue, retry, and reconciliation semantics.

The durable rule is short: hide transport differences, never hide accountability. A unified key can simplify the adapter; it cannot simplify a compliance decision that the organization has not made.

## References

- https://platform.openai.com/docs/guides/embeddings
- https://platform.openai.com/docs/api-reference/chat
- https://docs.anthropic.com/en/api/messages
- https://ai.google.dev/gemini-api/docs
- https://github.com/BerriAI/litellm
- https://www.rfc-editor.org/rfc/rfc9110
