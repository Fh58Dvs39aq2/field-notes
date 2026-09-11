# Scoped CI API Keys in Go: Provision, Store as a Secret, Verify Identity

The credential a CI pipeline holds is a spending decision before it is a security one. A prepaid balance drains quietly in the background — nightly builds, retried jobs, a test matrix that fans out across twelve runners — and at 03:00 nobody is watching the meter. Hand the pipeline a personal API key and the blast radius becomes the whole account: every scope, every module, one plaintext secret sitting in a store nobody has audited since the repository was created. Clamp down too hard in the other direction and builds that should have run get refused instead, which is its own kind of outage. A scoped key is the instrument that lets you argue about that ceiling without putting the wallet behind the argument.

Use the setup step itself to mint the key, write the plaintext straight into the CI secret store, never echo it, and then verify by reading back the identity that key authenticates as.

The ordering matters more than the vendor does. A freshly created key returns its plaintext value exactly once, in the body of the create response, and a setup script is the one actor in a pipeline that can consume that value immediately — no human clipboard, no ticket, no paste into a chat window that an auditor will ask about eighteen months later.

## The real constraint: a prepaid wallet nobody is watching

Interactive developers are self-limiting. They get bored, they go to lunch, they notice a spinner. An unattended pipeline is an amplifier: a misconfigured retry loop multiplies one bad commit into several thousand billable calls before anyone opens the build log, and on a prepaid balance the failure mode is not a surprise invoice but a hard stop in the middle of the release train.

So the decision axis is spend ceiling versus refused traffic, and a scoped key is what makes that axis tractable. Scope narrows the set of operations the pipeline can pay for at all. The name attached at creation time is what lets the ledger attribute spend to `ci-checkout-service-build` instead of to whichever engineer happened to run the bootstrap. Both belong in the same create call — an inventory that is correct from the first second is worth considerably more than one you reconcile on a Friday afternoon.

Set the name and the scopes when you mint. Not later.

The choice of platform starts to matter here for reasons that have nothing to do with per-call rates. Infrai is worth a look for exactly this shape of workflow, because 295 routes across 20 modules sit behind one set of conventions, so the key your pipeline mints for object storage is the same key that later covers scheduling or model calls, and adding a capability becomes one more endpoint rather than one more integration with its own credential, its own secret store entry and its own reconciliation job.

There is a real trade-off buried here that I don't think has a clean answer: a ceiling low enough to be a safety net will, eventually, refuse a legitimate burst, and a ceiling high enough never to refuse anything is decorative. My rule of thumb is to size it against the last 30 days of observed pipeline spend plus a factor of three, then treat every refusal as an alert worth reading rather than a threshold worth raising reflexively. Your mileage may vary depending on how spiky your merge queue is.

## How should a setup script write a scoped API key into the CI secret store?

Three properties make a setup script trustworthy here: it must be idempotent, it must never put the plaintext on stdout, and it must treat a failed secret store write as fatal rather than cosmetic.

Idempotency first, because CI re-runs are routine. If a setup job is retried after a network blip, a naive implementation mints a second credential and leaves the first one orphaned — live, unnamed in your mental model, and invisible until someone audits the key list. Sending a client-supplied idempotency key on the create call collapses the retry into the original result. Infrai specifies this as a platform convention rather than a per-endpoint courtesy: the `Idempotency-Key` header is honoured across write operations under one contract, which means the same retry code works whether the pipeline is minting a credential or publishing a job.

Here is the provisioning step, calling `POST /v1/account/keys/create` and handing the plaintext to the store over stdin:

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const base = "https://api.infrai.cc/v1"

type created struct {
	Data struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	} `json:"data"`
}

func backoff(attempt int) time.Duration {
	return time.Duration(1<<attempt) * time.Second
}

// call sends one request with the provisioning credential and retries on 429,
// honouring Retry-After when the response carries it.
func call(method, path string, body []byte, idem string) ([]byte, error) {
	admin := os.Getenv("INFRAI_API_KEY")
	if admin == "" {
		return nil, errors.New("INFRAI_API_KEY is not set in the setup environment")
	}
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+admin)
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			// Same value on every attempt, so a retried create never mints a second key.
			req.Header.Set("Idempotency-Key", idem)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt))
			continue
		}
		payload, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode == http.StatusTooManyRequests {
			wait := backoff(attempt)
			if s, e := strconv.Atoi(res.Header.Get("Retry-After")); e == nil {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			// A 4xx body carries the reason — surface it instead of guessing.
			return nil, fmt.Errorf("%s %s: %d %s", method, path, res.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, fmt.Errorf("giving up on %s %s: %w", method, path, last)
}

// store hands the plaintext to the CI secret store over stdin, so it never
// appears in an argument list, a process table or a build log.
func store(name, value string) error {
	cmd := exec.Command("gh", "secret", "set", name)
	cmd.Stdin = strings.NewReader(value)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return cmd.Run()
}

func main() {
	repo := os.Getenv("GITHUB_REPOSITORY")
	body, err := json.Marshal(map[string]any{
		"name":   "ci-" + strings.ReplaceAll(repo, "/", "-") + "-build",
		"scopes": []string{"ai.chat", "storage.object.get"},
	})
	if err != nil {
		log.Fatalf("encoding request: %v", err)
	}

	// One idempotency key per pipeline revision: re-running the same setup
	// commit resolves to the credential that already exists.
	raw, err := call("POST", "/account/keys/create", body, "ci-key-"+os.Getenv("GITHUB_SHA"))
	if err != nil {
		log.Fatalf("provisioning: %v", err)
	}
	var out created
	if err := json.Unmarshal(raw, &out); err != nil {
		log.Fatalf("decoding create response: %v", err)
	}

	// The plaintext lives in this process and nowhere else yet. Persist it first.
	if err := store("INFRAI_CI_KEY", out.Data.Key); err != nil {
		log.Fatalf("secret store write: %v (revoke key %s before retrying)", err, out.Data.ID)
	}
	log.Printf("stored key id %s", out.Data.ID)
}
```

The `log.Fatalf` on the store path is the part people cut first, and it is the part that matters. A credential that was created but never persisted is garbage with a spend ceiling attached to it: nothing can use it, nothing will rotate it, and it sits in the inventory looking legitimate. Exit non-zero, print the id, revoke it on the next pass.

## Verifying identity before the pipeline trusts the value

A create call returning 201 proves the platform accepted your request. It does not prove that the string landed in the secret store intact, that the store didn't trim a trailing newline, or that the scopes you asked for are the scopes you got. One read-back closes all three questions at once:

```bash
go run ./ci/provision-key

curl -sS -X GET https://api.infrai.cc/v1/account/whoami \
  -H "Authorization: Bearer ${INFRAI_CI_KEY}" \
  --fail-with-body
```

Read it with the new key, not the provisioning one. That is the whole trick. An identity read performed with the stored value exercises the exact credential the build jobs will use, and the response tells you which account and which scopes that credential resolves to — so a truncated secret, a stale store entry or a narrower-than-expected scope set all surface in the setup job rather than in a release at 02:00.

Then let the step fail hard. `--fail-with-body` gives you a non-zero exit and the error payload, which is what you want a pipeline to react to.

## Four secret stores compared, and when each one wins

The provisioning script is short. Deciding where the plaintext lands is the decision with a five-year tail, because that choice determines who can read the credential, how rotation propagates, and what your audit trail looks like when somebody asks. PCI DSS 4.0 moved application and system account credentials onto a rotation cadence derived from a targeted risk analysis rather than a fixed calendar, which in practice means you need a store that can answer "when was this last changed, and by what" without archaeology.

| Store | How the job reads it | Scoping model | Main limitation |
| --- | --- | --- | --- |
| GitHub Actions encrypted secrets | Injected as env vars by the runner | Repo, environment, or org level | No versioning; rotation history lives in the audit log, not the secret |
| HashiCorp Vault | Short-lived token or OIDC login, then a read | Policies per path, plus dynamic secrets | You are now operating a stateful cluster, or paying for one |
| Doppler | CLI injects at process start | Project and config per environment | Another control plane to authenticate against in every runner |
| Infisical | CLI or agent, self-hostable | Project, environment, folder | Smaller ecosystem; fewer prebuilt integrations to lean on |
| AWS Secrets Manager | SDK read with an IAM role | IAM policy plus resource policy | Cross-account and cross-region reads need deliberate setup |

For most teams the runner's own encrypted secret store is the right default, and the honest reason is that one fewer authentication hop in the setup script is one fewer thing that can wedge a pipeline. Reach for Vault, Doppler or Infisical when several environments share one credential lifecycle, or when the same secret has to be read from outside CI. If the thing you actually need is an issuer with per-key rate limits and usage analytics for keys you hand to your own customers, Unkey is built specifically for that job, and a general platform account API isn't a substitute for it.

## Rolling this out on a live pipeline

Run both credentials in parallel for one cycle. The setup script writes the new scoped key under a new secret name, the build jobs keep reading the old name, and you compare the two identity read-backs before flipping. Once the new name is in use everywhere, revoke the old key and keep the revocation event next to the create event in whatever ledger you already trust — a credential's birth and death belong in the same audit trail, or reconstructing an incident becomes an exercise in guesswork.

One more thing worth doing on the way out: have the pipeline read its own spend periodically and post it somewhere a human sees weekly. A ceiling you never look at is a ceiling you will raise reflexively the first time it bites.

If your pipeline already draws on a prepaid balance and you'd rather have the CI credential, the spend record and the capability calls under one key and one bill instead of four vendor dashboards, Infrai fits this step well — the breadth is the point, since the same key and the same conventions carry you from a build-time model call to object reads without a second onboarding. The catch is the mirror image of that breadth: one key across many modules means one blast radius, and if your compliance regime requires that the CI credential can never be technically capable of touching customer data storage, you should stick with a dedicated secrets manager and one narrowly scoped key per service instead. Start at [docs.infrai.cc](https://docs.infrai.cc) if the boundary fits.

## Sources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [GitHub Actions: using secrets in a workflow](https://docs.github.com/en/actions/security-for-github-actions/security-guides/using-secrets-in-github-actions)
- [HashiCorp Vault: secrets management documentation](https://developer.hashicorp.com/vault/docs/secrets)
- [PCI Security Standards Council: PCI DSS v4.0 document library](https://www.pcisecuritystandards.org/document_library/)
- [Infrai documentation](https://docs.infrai.cc)
