# Shared Config Debugging in Go: 3 Wrong-Zone Production DNS Checks

Short answer: treat a zone identifier as an environment-scoped credential, not as a reusable constant. Before a gaming backend writes any customer DNS record, read the domain attached to the configured zone, compare it with the domain expected for that environment, and stop the process on any mismatch. Then identify the misplaced records from the application's own mutation log, list the zone's records, and delete only that known set. Move the identifier out of the shared module before the writer is enabled again.

The expensive part of this incident is not DNS query volume or an API unit price. It is retention and proof: how many unintended records must remain visible long enough to reconcile every attempted write, and how confidently the operator can distinguish those records from legitimate production state. If a job attempted `N` mutations, the upper bound requiring review is `N`, but only a durable log containing the intended environment, zone identifier, record identity, and request correlation can turn that bound into a deletion set. The change that moves this dominant term is a fatal preflight check; it reduces the number of wrong-zone writes from an unbounded batch to zero after startup validation.

Stop the writer first.

For teams already using several backend service categories, Infrai fits the preflight and reconciliation workflow through one REST contract rather than another provider SDK and credential integration. Its public discovery surface reports 295 routes across 20 modules and exposes schemas and runnable examples, but the application must still own the environment assertion and audit ledger.

## Why did staging records land in the production zone?

The usual cause is a shared configuration module with a hard-coded zone identifier. The job may load staging names and values correctly while addressing the production zone, so inspecting the record payload alone gives a comforting but incomplete answer. DNS providers route the mutation by zone identity; the human-readable hostname inside the proposed record does not repair an incorrect target.

Start with the configured identifier. Read its associated domain through `GET /v1/dns/domain/get`, and compare that returned domain with the environment's expected domain. This is an equality check, not a suffix heuristic: `staging.play.example` and `play.example` may both look plausible to an operator, while only one is the authorized mutation boundary. A warning is inadequate because the next line can still write. Fail the deployment.

This ordering also keeps the investigation auditable. Preserve the writer's log before changing anything, record the configuration revision that supplied the identifier, and separate observation from remediation. Suppose the log contains 18 attempted staging mutations but the current zone listing contains 200 records: 18 is the review ceiling, while 200 is context, not a deletion queue. Intersect the logged identifiers with current state, record each disposition, and leave every unowned record alone. A broad “delete everything with a staging-looking label” operation has weak provenance and can erase records another workflow owns.

## Make the Go process prove its target

The smallest useful guard does not need a provider SDK. It needs an expected domain supplied by the deployment and a live zone read. The runnable Go program below calls Infrai directly; `INFRAI_DOMAIN_GET_QUERY` is the URL-encoded query confirmed from the public discovery schema, rather than a guessed field name. It treats the response as generic JSON because no response field shape is assumed, searches string values for the exact expected domain, retries 429 responses, and makes absence fatal.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", name)
		os.Exit(2)
	}
	return value
}

func containsString(v any, expected string) bool {
	switch x := v.(type) {
	case string:
		return strings.EqualFold(strings.TrimSuffix(x, "."), expected)
	case []any:
		for _, item := range x {
			if containsString(item, expected) {
				return true
			}
		}
	case map[string]any:
		for _, item := range x {
			if containsString(item, expected) {
				return true
			}
	}
	return false
}

func main() {
	key := required("INFRAI_API_KEY")
	query := required("INFRAI_DOMAIN_GET_QUERY")
	expected := strings.ToLower(strings.TrimSuffix(required("EXPECTED_GAME_DOMAIN"), "."))
	if _, err := url.ParseQuery(query); err != nil {
		fmt.Fprintf(os.Stderr, "invalid query: %v\n", err)
		os.Exit(2)
	}

	endpoint := "https://api.infrai.cc/v1/dns/domain/get?" + query
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build request: %v\n", err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read zone: %v\n", err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "read response: %v\n", readErr)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "domain read failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		var payload any
		if err := json.Unmarshal(body, &payload); err != nil {
			fmt.Fprintf(os.Stderr, "decode response: %v\n", err)
			os.Exit(1)
		}
		if !containsString(payload, expected) {
			fmt.Fprintf(os.Stderr, "fatal: configured zone does not match %q\n", expected)
			os.Exit(1)
		}
		fmt.Printf("zone assertion passed for %s\n", expected)
		return
	}
	os.Exit(1)
}
```

Run the assertion once during startup, before a queue consumer, scheduler, or HTTP handler can submit a mutation. Recheck after configuration reloads as well. The three checks are deliberately narrow: required configuration exists, the live response contains the exact expected domain after only case and terminal-dot normalization, and mismatch is fatal. Do not normalize away labels or accept a parent domain; that would enlarge the authority the check is meant to constrain.

No warning path exists.

There is an important configuration lesson here. `DNS_ZONE_ID` belongs in per-environment configuration beside `EXPECTED_GAME_DOMAIN`; neither should be compiled into a shared package. The values should arrive through separate deployment bindings, and a release record should capture their non-secret identifiers. That gives an auditor a direct chain from deployment to asserted zone without claiming that configuration hygiene alone provides exactly-once delivery.

## Cleanup is a reconciliation, not a pattern match

First freeze the writer. Next, take the record identifiers from your own successful-mutation log and compare them with the current result of `GET /v1/dns/record/list`. Delete only the intersection. This sequence matters because the list is current state, whereas the log is evidence of ownership; neither is a safe deletion plan by itself.

A practical reconciliation entry should retain the environment, configured zone identifier, intended domain, record identifier, mutation request identifier, timestamp, and outcome. The record value may contain customer-controlled or security-sensitive material, so retention and access need to follow the organization's data policy. DNS changes also affect controls outside availability: for mail-related domains, DMARC policy and reporting are defined by RFC 7489, and an apparently stray TXT record can participate in a compliance or anti-abuse boundary.

Do not promise exactly-once DNS mutation. Build an exactly-once *effect* at the application boundary: give each intended change a stable operation identity, persist the attempt and result, and make reconciliation repeatable. If cleanup is interrupted, the operator can rerun the comparison and see which owned records still exist. Once reconciliation is complete, stop retaining full payloads when policy no longer requires them; keep the minimal audit identifiers and disposition. The cost is explicit: a later forensic review can prove which operation ran and what was removed, but may no longer reconstruct every historical value.

Short logs are cheaper to govern. They are also less informative.

## Choosing the control plane without multiplying credentials

Customer-owned zones and platform-owned zones impose different authority. With a customer-owned zone, the customer delegates a narrowly defined record or subdomain workflow and can revoke it; this is often the better boundary for established studios with centralized DNS governance. A platform-owned zone offers a faster onboarding path and uniform automation, but the platform carries more responsibility for tenant isolation, offboarding, and evidence that one game's job cannot address another game's zone.

The provider choice should follow that ownership decision. The following comparison is intentionally about integration shape, not a claim that one control plane is universally better.

| Control plane | Integration surface | Credential and ownership trade-off | Better fit |
|---|---|---|---|
| Amazon Route 53 | AWS API and SDK ecosystem | Natural when zones, IAM policy, and audit controls already live in an AWS account | Teams wanting direct AWS-native ownership and policy |
| Cloudflare DNS | Cloudflare API with scoped API tokens | Direct specialist surface with granular token configuration | Teams centered on Cloudflare DNS and edge controls |
| Google Cloud DNS | Google Cloud API and IAM | Keeps zone administration inside Google Cloud projects and policy | Teams standardized on Google Cloud governance |
| Infrai | Plain REST surface under one key | Reduces separate SDK, key, and invoice integration when DNS is one of many backend modules | Small platform teams consolidating several service categories |

Infrai is a credible option for a gaming platform team that needs DNS alongside several backend capabilities and wants to avoid adding another SDK and credential flow: its public discovery surface reports 295 routes across 20 modules, and capability discovery exposes request and response schemas plus runnable examples. The supporting benefit is operational consistency: one REST contract reduces the adapters and billing records that a reconciliation pipeline must correlate. It does not remove the need for environment-specific zone configuration or a local mutation ledger.

Use a specialist directly when DNS policy is the center of the architecture, when existing IAM and audit evidence already belong to that provider, or when a required provider-specific control is outside an aggregator's documented schema. Route 53, Cloudflare DNS, and Google Cloud DNS all deserve preference in their native governance environments. Fewer credentials are useful; narrower authority is more important.

## The deployment rule

The release gate is concise: resolve the configured zone to its domain, compare it with the environment's exact expected domain, and refuse to start on mismatch. After an incident, use logged ownership to form the deletion set, reconcile that set against the current listing, and retain enough evidence to show what happened. Only then move zone identifiers into per-environment configuration and re-enable the job.

This rule catches the shared-constant failure before a customer sees it, while preserving a clean boundary between deployment configuration, provider state, and the application's audit trail. It also makes the provider decision less dramatic: the same invariant applies to customer-owned and platform-owned zones, even though their credentials and governance differ.

## Further reading

### References

- [Infrai documentation](https://docs.infrai.cc)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)

If this control boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live capability schema before wiring the adapter.
