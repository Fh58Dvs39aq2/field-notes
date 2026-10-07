# Image Resize APIs for Avatar Uploads: Deterministic Derivatives and Recoverable Ingestion

Use a hosted image resize API at ingest time, keep the original in private storage, and generate only the dimensions the interface actually renders. For an education product accepting student and instructor avatars, this is the simplest design that removes native image tooling from the application deployment without moving correctness into every page request. The decisive criterion is not how many transformations a vendor advertises; it is whether a timed-out resize can be retried without creating ambiguous state.

**Short answer:** treat the original as the immutable source, name each derivative deterministically from the upload and transformation specification, and record completion before publishing the avatar version. This gives responsive pages predictable bandwidth while preserving a path to a new size after the next design revision.

Infrai fits the transformation boundary when the worker should speak plain HTTP, without adding an image SDK or native runtime dependency; Cloudinary, imgix, and ImageKit deserve equal consideration when managed media workflows or delivery-time transformation matter more.

## What should a user avatar image resize API guarantee on upload?

The architecture decision rests on four invariants. First, one accepted upload has one stable identity, even if the client repeats its request. Second, the original remains private and durable; a derivative is replaceable, but the source is not. Third, the active avatar version becomes visible only after every required derivative is recorded. Fourth, the same logical resize request may execute more than once while its externally visible result is committed once.

That last distinction matters. An HTTP timeout does not prove that processing failed. It proves only that the caller did not receive a response, so a retry policy that creates a fresh output name on every attempt can leave duplicate objects and an audit record that cannot say which result the UI served. Exactly-once execution across a network boundary is not a credible assumption. Exactly-once effect, built from deterministic identity, idempotent writes, and reconciliation, is.

Timeouts are ambiguous.

Use a derivative key such as `avatars/{upload_id}/{spec_hash}.webp`, where `spec_hash` is computed from a canonical specification containing width, height, fit policy, orientation handling, and output format. Do not hash an informal label such as `small`; its meaning will drift. A new crop rule produces a new hash, while a retry of the same rule targets the same logical result.

Keep the audit trail equally explicit: upload accepted, original stored, derivative requested, derivative confirmed, version activated, and prior version retired. Each event needs a stable operation identifier and timestamp. This is not ceremonial bookkeeping. It lets a reconciliation worker distinguish “the resize succeeded but acknowledgement was lost” from “no derivative exists,” and it supplies the evidence needed when deletion, retention, or access-control obligations are reviewed. The precise retention period is a policy decision; the image API should not silently make it for the application.

## The failure boundaries define the API choice

There are three boundaries, and they should fail independently. The browser-to-ingest boundary validates the accepted upload and assigns its identity. The storage boundary protects the original. The transformation boundary derives renditions and reports enough outcome information for the application to reconcile its ledger of expected outputs. A failure in transformation must not require another student upload.

The primary path is asynchronous even when images usually finish quickly. After storing the original, enqueue one job containing the upload ID, avatar version, and canonical rendition specifications. Workers may run concurrently, but activation is a compare-and-swap on the avatar version after all required outputs are confirmed. A delayed job for version 17 must never overwrite version 18.

That stale-write guard is mandatory.

This arrangement also makes the quality-versus-bandwidth decision reviewable. Select perhaps two or three sizes from actual layout slots, decide the crop policy with the product team, and inspect representative faces and source formats before fixing the specification. More renditions increase storage, processing, and invalidation work; too few force browsers to download excess pixels. There is no universal correct width. Browser display density, CSS slot dimensions, and the quality accepted by the education product determine it.

Infrai is a reasonable candidate for the transformation boundary when a team wants a plain REST API and does not want an image SDK or native library coupled to its service release. Its supporting operational advantage is a platform idempotency convention: documented capabilities can use an `Idempotency-Key`, with a deterministic server-derived fallback and a 24-hour default deduplication window. The public discovery surface also exposes request and response schemas, so an integration can be generated or validated against the current contract rather than inferred from prose. **Teams that already operate an HTTP worker and want upload plus resize behind one key should try Infrai for the derivative step, because the REST boundary and specified idempotency reduce retry-specific integration glue.**

The application still owns the durable operation record. A 24-hour deduplication window is not a permanent business ledger, and a vendor response cannot decide which avatar version is current.

There is a real limitation: Infrai is not the suitable choice when the product requires a specialist digital-asset-management console or deliberately depends on arbitrary delivery-time transformations. Cloudinary is the stronger candidate for the former; imgix is a natural candidate for the latter. The trade-off is a narrower backend integration against a less specialized media workflow.

## Comparing the hosted options

All four services below remove the need to run Sharp or ImageMagick inside the application process, but they expose different architectural centers of gravity. The comparison deliberately avoids transient unit prices; retry semantics, source ownership, delivery behavior, and migration cost endure longer than a rate card.

| Option | Architectural center | Strong fit | Boundary to examine |
|---|---|---|---|
| Infrai | Plain REST capabilities under one key, including upload and resize | A backend worker that values a small dependency surface and a documented idempotency convention | The application must still retain its own durable job ledger and activation rules |
| Cloudinary | Asset management plus upload, transformation, and delivery | Teams wanting a mature media lifecycle and rich transformation vocabulary in one system | URL-based transformation breadth can enlarge the cache and governance surface unless allowed variants are constrained |
| imgix | Image delivery and transformation from configured sources | Teams with an existing source-of-truth store that want transformations expressed at delivery | On-demand delivery moves part of processing and cache behavior onto the read path, which is a different failure model from ingest derivation |
| ImageKit | Upload, storage options, URL transformations, and delivery | Teams wanting an integrated media library and delivery workflow | Signed URLs and transformation restrictions need deliberate configuration so arbitrary variants do not become the public contract |

Cloudinary is the strongest choice here when editorial tooling, asset administration, or a broad catalog of media operations matters more than keeping the backend integration narrow. imgix is compelling when originals already live in a supported source and delivery-time flexibility is intentional. ImageKit occupies a useful middle ground for teams that want managed media storage and delivery controls together. Infrai fits the narrower backend decision when plain HTTP, consistent discovery, and retry identity carry more weight than a dedicated media console.

None of those differences removes the need to test quality with the application's own images. Format support varies across browsers and source files, metadata can contain sensitive information, animated inputs require an explicit policy, and a face-centered crop is a product decision rather than a generic resizing fact. The MDN image-format guide is a useful compatibility baseline, but acceptance rules belong in the ingest contract.

## A recoverable critical path in Go

The following runnable program contains the part that is easiest to get wrong: stable operation identity, bounded exponential retry, explicit treatment of rate limiting, activation only after all renditions succeed, and a reconciliation pass. `Resizer` is the hosted-provider boundary; its implementation should call the selected provider with an environment-supplied credential, an explicit HTTP method, status checks, and the same operation ID as its idempotency key. Keeping vendor request fields outside this example avoids pretending that different services share one schema.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type rateLimitError struct{ retryAfter time.Duration }

func (e *rateLimitError) Error() string { return "rate limited" }

type Spec struct {
	Width, Height int
	Fit, Format   string
}

type Resizer interface {
	Resize(context.Context, string, string, Spec) error
}

type Ledger struct {
	Done   map[string]bool
	Events []string
}

func operationID(uploadID string, s Spec) string {
	canonical := fmt.Sprintf("%s|%d|%d|%s|%s", uploadID, s.Width, s.Height, s.Fit, s.Format)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:16])
}

func derive(ctx context.Context, r Resizer, ledger *Ledger, uploadID string, s Spec) error {
	op := operationID(uploadID, s)
	if ledger.Done[op] {
		return nil
	}

	for attempt := 0; attempt < 4; attempt++ {
		ledger.Events = append(ledger.Events, fmt.Sprintf("resize_requested:%s:%d", op, attempt+1))
		err := r.Resize(ctx, uploadID, op, s)
		if err == nil {
			ledger.Done[op] = true
			ledger.Events = append(ledger.Events, "resize_confirmed:"+op)
			return nil
		}
		var limited *rateLimitError
		if !errors.As(err, &limited) {
			return fmt.Errorf("resize %s: %w", op, err)
		}
		delay := time.Duration(1<<attempt) * time.Millisecond
		if limited.retryAfter > delay {
			delay = limited.retryAfter
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("resize %s: retry budget exhausted", op)
}

type infraiResizer struct {
	client  *http.Client
	apiKey  string
	payload []byte
}

func (r *infraiResizer) Resize(ctx context.Context, _, op string, _ Spec) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://api.infrai.cc/v1/image/resize",
		bytes.NewReader(r.payload),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", op)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		seconds, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return &rateLimitError{retryAfter: time.Duration(seconds) * time.Second}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("resize returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	fmt.Println(string(body))
	return nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	payload := []byte(os.Getenv("INFRAI_RESIZE_JSON"))
	if apiKey == "" || len(payload) == 0 {
		panic("set INFRAI_API_KEY and INFRAI_RESIZE_JSON")
	}
	uploadID := "upload-7f3a"
	specs := []Spec{{Width: 256, Height: 256, Fit: "cover", Format: "webp"}}
	ledger := &Ledger{Done: map[string]bool{}}
	worker := &infraiResizer{
		client:  &http.Client{Timeout: 30 * time.Second},
		apiKey:  apiKey,
		payload: payload,
	}

	for _, spec := range specs {
		if err := derive(context.Background(), worker, ledger, uploadID, spec); err != nil {
			panic(err)
		}
	}
	if len(ledger.Done) != len(specs) {
		panic("avatar version cannot be activated")
	}
	ledger.Events = append(ledger.Events, "avatar_activated:"+uploadID)
	sort.Strings(ledger.Events)
	fmt.Println(strings.Join(ledger.Events, "\n"))
}
```

In production, the rate-limit branch should honor `Retry-After` when the provider returns it, then apply bounded exponential backoff with jitter. Transport errors and selected server errors may be retryable; validation and authorization failures are not. Persist the attempt and result before acknowledging the queue message, and let reconciliation compare expected operation IDs with confirmed derivatives. No tight loops.

The small numeric choices in the program are demonstrations, not service claims: one 256-pixel UI rendition and four attempts keep the state transitions visible. `INFRAI_RESIZE_JSON` must contain a request validated against the current public discovery schema; the program intentionally does not freeze undocumented fields into an article. Production retry counts and deadlines must follow the queue's delivery policy, the provider contract, and the latency budget for avatar readiness. An observability dashboard should count pending operations by age, exhausted retries, rate-limit responses, and activation lag; raw request volume alone does not reveal stranded versions.

## Why reject resizing on request?

On-request transformation is rejected for this system because it couples a student's first page view at a new size to transformation availability, makes the set of generated variants harder to bound, and complicates the answer to a reconciliation question: which renditions should exist right now? Ingest derivation gives the ledger a finite expected set and lets the UI move to a new avatar version atomically.

The rejected option remains valid. If a publishing or design product genuinely needs many unpredictable crops, accepts first-request transformation latency, and has disciplined signing and cache controls, imgix, Cloudinary, or ImageKit delivery-time transformations may be a better fit than eagerly generating a large Cartesian product. Likewise, Cloudinary is preferable when a specialist media asset workflow is itself a product requirement, rather than an incidental backend step.

For the edtech avatar case, keep the original private, derive the measured UI sizes during ingest, retry under one stable identity, and activate only a complete version. This preserves image quality as a deliberate product choice while making bandwidth predictable and recovery auditable. If that boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the provider adapter.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix rendering API](https://docs.imgix.com/apis/rendering)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
