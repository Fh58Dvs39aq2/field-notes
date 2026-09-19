# Prepaid API Continuity Explained: Service Stopped When Auto Recharge Lacks a Default

An enabled auto-recharge rule does not guarantee continuity: it authorizes a policy, while an eligible default payment method, a usable credential, successful settlement, and balance propagation must all work before service can continue. **Treat funding and API-key rotation as separate state machines with bounded failure domains.** For a B2B SaaS production service, first preserve authenticated access, then trace the last recharge attempt from threshold evaluation through payment-method selection and ledger posting; do not respond to a funding failure by hurriedly replacing the only credential used by every workload.

Short answer: confirm that the account has a default payment method eligible for the intended charge, inspect the recharge attempt's recorded state and reason, reconcile the payment result against the prepaid ledger, and rotate keys with an overlap period rather than an instantaneous swap. Auto-recharge is a trigger, not a stored-value guarantee.

## How can the service stop even though API auto recharge is configured?

The useful mental model is a chain of explicit preconditions. A balance crossing its threshold may create an intent to recharge, but that intent still needs an account in good standing, a selectable default payment method, authorization by the payment processor, a terminal payment result, and a corresponding credit in the prepaid ledger. A missing default breaks the chain before a charge can be attempted. A declined or still-pending charge breaks it later. A successful charge without a posted credit indicates a reconciliation problem rather than a card-selection problem.

Those distinctions matter because the remedies are different. Repeatedly toggling the rule cannot supply a missing payment method, and repeatedly charging after an ambiguous timeout can create duplicate financial effects. The correct response is to identify the last durable transition and resume from there with the same idempotency key. Guessing is expensive.

Stop there.

Keep three identifiers together in the audit trail: the recharge intent ID, the processor operation ID, and the ledger transaction ID. The audit record should also capture the policy version, threshold observation, selected payment-method reference, actor, timestamps, and terminal reason. Do not log the secret API key or full payment credentials. OWASP recommends centralized secret lifecycle management, least privilege, rotation, revocation, and auditing; those controls apply to the production key used during this investigation as much as they do during routine deployment.

## Diagnose the state transition, not the checkbox

Start with evidence that cannot mutate the account. Read the current available balance and any reserved amount, locate the most recent threshold evaluation, and inspect the linked recharge intent. If no intent exists after the threshold was crossed, investigate rule scope, activation time, and the balance snapshot consumed by the evaluator. If an intent exists but has no payment-method reference, verify the account-level default and whether it is valid for that transaction context. The phrase “payment method on file” is weaker than “eligible default selected for this charge.”

Next, separate processor truth from ledger truth. A terminal failure with a reason should remain failed; after correcting the default, create or resume work according to the system's documented retry semantics. An unknown result requires lookup by the original idempotency key or processor reference before any retry. A successful payment must map to exactly one ledger credit. Reconciliation should flag zero credits and multiple credits alike.

Use a compact decision table during an incident:

| Durable evidence | Likely boundary | Next action |
|---|---|---|
| No recharge intent | Policy evaluation | Verify threshold snapshot, rule scope, and activation |
| Intent has no method reference | Account configuration | Set or repair the eligible default, then resume safely |
| Processor result is unknown | Payment integration | Query by the original operation reference before retrying |
| Payment failed terminally | Payment authorization | Correct the reported cause; do not blind-retry |
| Payment succeeded, no ledger credit | Accounting pipeline | Reconcile and post once under the original intent |
| Ledger credit exists, access remains blocked | Entitlement propagation | Refresh derived state and verify consumer checkpoints |

This ordering prevents a common category error: treating the visible shutdown as proof that auto-recharge never ran. It may have run and stopped at a later boundary. The audit trail decides.

## Contain the credential blast radius

The production API key presents a parallel continuity risk. If one credential is shared by the web service, workers, scheduled reconciliation, and administrative tooling, revoking it turns a narrow billing investigation into a fleet-wide outage. **One credential should have one operational owner and the smallest practical workload scope.** Separate credentials also make audit events attributable and permit one consumer to rotate without synchronizing every deployment.

Rotation needs an overlap window because distributed deployments do not change atomically. Create the successor credential through an authorized control path, store it in the secret manager, deploy consumers so they can authenticate with the new value, and observe successful traffic by credential identifier. Only then revoke the predecessor. Keep the overlap bounded; two indefinitely valid secrets double exposure without completing the rotation.

This design has a real trade-off. Per-workload credentials increase secret inventory, ownership records, alert cardinality, and rotation work, so they are a poor fit for a single-process prototype with no independent deployment units. A shared credential is simpler there. Once workers, reconciliation jobs, and serving processes deploy separately, however, that simplicity becomes coupled failure: the administrative overhead of scoped keys is justified by the smaller revocation blast radius. Dual-key overlap also has a limitation: it cannot protect a system whose upstream accepts only one active credential. Such a boundary requires a staged proxy or coordinated maintenance procedure, and calling that process “zero downtime” would be misleading.

The application should never persist raw keys in its business database or emit them to logs. It can record a non-secret key identifier, version, creation time, intended workload, and retirement state. That is enough to answer the operational questions: which deployment is still using the old key, which actor initiated rotation, and whether revocation occurred after adoption.

Here is a small Go model for the decision boundary. It deliberately separates observation from mutation and makes ambiguous payment outcomes non-retryable until reconciled.

```go
package recharge

import "fmt"

type PaymentState string

const (
	NotAttempted PaymentState = "not_attempted"
	Pending      PaymentState = "pending"
	Succeeded    PaymentState = "succeeded"
	Failed       PaymentState = "failed"
	Unknown      PaymentState = "unknown"
)

type Snapshot struct {
	BelowThreshold   bool
	DefaultMethodID string
	Payment         PaymentState
	LedgerCredits   int
}

func NextAction(s Snapshot) (string, error) {
	if !s.BelowThreshold {
		return "no recharge required", nil
	}
	if s.DefaultMethodID == "" {
		return "repair eligible default payment method", nil
	}
	switch s.Payment {
	case NotAttempted:
		return "create charge with a stable idempotency key", nil
	case Pending, Unknown:
		return "reconcile original payment operation", nil
	case Failed:
		return "resolve terminal payment failure", nil
	case Succeeded:
		if s.LedgerCredits == 0 {
			return "post one ledger credit using the recharge intent", nil
		}
		if s.LedgerCredits == 1 {
			return "verify entitlement propagation", nil
		}
		return "freeze automation and investigate duplicate credits", nil
	default:
		return "", fmt.Errorf("unsupported payment state %q", s.Payment)
	}
}
```

The function is not a payment integration. Its value is the invariant it exposes: a pending or unknown external effect must be reconciled before another effect is requested, and a successful charge may produce exactly one credit. In a real service, enforce uniqueness on the recharge intent at the ledger boundary and retain the mapping needed for later audit.

## Test recovery as a financial workflow

A health check that merely confirms HTTP 200 responses misses the dangerous cases. Exercise the workflow with a threshold crossing and no default method, a terminal decline, an ambiguous processor response, a duplicate delivery, and a successful credit whose entitlement update is delayed. For each case, assert both the customer-visible access state and the ledger entries. Then replay the same event. Nothing financial should change on replay.

Test key rotation independently: old key only, both keys during overlap, new key only after revocation, and an intentionally stale worker. The stale worker should fail clearly while the rest of the service continues. This is where credential separation earns its operational cost.

One stale worker is enough.

Observability should follow transitions rather than secrets. Useful measures include recharge intents by terminal state, time spent pending, successful payments lacking a ledger credit, credits lacking an entitlement update, and authentication failures grouped by non-secret key identifier. Alerts need correlation IDs that lead an operator from access denial to recharge intent, payment reference, and ledger transaction without exposing credential material.

Compliance scope constrains the implementation. PCI DSS requires protection of stored account data and restricts retention of sensitive authentication data after authorization; using processor-issued references instead of storing raw card data reduces what the application handles, but the actual scope depends on the data flow and must be assessed. Audit records should therefore contain references and decisions, not payment secrets. Retention must follow the organization's legal and compliance policy rather than an arbitrary debugging preference.

## Roll out the repair without another interruption

Repair the eligible default payment method through an authorized administrative path, reconcile the existing recharge intent, and confirm one ledger credit before restoring any balance-dependent entitlement. In parallel, issue a workload-scoped successor API key, deploy it first to a small consumer set, verify authentication by key identifier, expand deployment, and revoke the predecessor only after no expected consumer uses it.

Keep rollback asymmetric. Application deployments may roll back, but a completed ledger credit should not be “rolled back” by deleting history; use a compensating entry when correction is required. Likewise, do not resurrect a revoked credential merely because an old deployment reappears. Fix the deployment and preserve the security event.

The durable design rule is compact: make every external financial operation idempotent, make every accounting change append-only and reconcilable, and make every production credential replaceable within a narrow blast radius. **Continuity comes from explicit state and controlled overlap, not from an enabled toggle.**

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://www.pcisecuritystandards.org/document_library/
- https://datatracker.ietf.org/doc/html/rfc6749
