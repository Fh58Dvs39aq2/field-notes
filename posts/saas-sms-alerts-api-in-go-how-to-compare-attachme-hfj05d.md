# SaaS SMS Alerts API in Go: How to Compare Attachment Dispatch Evidence

To compare an SMS alerts API for marketplace SaaS, choose the dispatch design by the evidence it preserves when the outcome is uncertain. For generated settlement reports sent as email attachments, keep the report notification in an application-owned journal and treat any accompanying SMS alert as a separate obligation.

TL;DR: Commit the report identity and intended recipient before dispatch, assign one stable key to each notification obligation, and retain an explicit unknown state when an attempt may have reached the transport. Reconcile that state before authorizing another submission. Compare SMS alerts APIs for US and EU SaaS against this workflow before comparing expenditure; a low quoted rate cannot establish whether a particular report reached its intended recipient.

This decision record takes an audit-evidence angle: the output is a defensible account of what was requested, attempted, and observed. Delivery reliability remains the acceptance criterion. The concrete job is delivering the generated report as an email attachment; the SMS is an optional notification about that report, with its own destination and delivery evidence.

Keep them separate.

## 1. What must the journal prove before a report is sent?

Begin by defining the business obligation. In this design, an obligation binds a marketplace, a settlement period, an immutable report revision, a recipient snapshot, and a channel. A corrected report receives a new revision and a new obligation. Replaying an existing obligation preserves its identity. This is the same correctness discipline that makes ledger reconciliation tractable: identifiers describe business intent, while individual attempts describe execution.

For example, the illustrative key `marketplace-42/settlement-17/report-r3/email/seller-8` identifies one attachment notification. These numbers are sample inputs, not measured production results. An SMS notification uses a different channel component. A recipient change must be an explicit new obligation or an explicitly authorized amendment, because silently replacing a destination destroys the meaning of the original audit record.

The invariants are narrow. Persist the obligation before making an external call. Record an attempt before that call begins. Preserve evidence without converting transport acceptance into confirmed delivery. Never let the SMS state mark the email obligation complete.

Uncertainty needs a name.

A timeout after submission leaves two possibilities: the transport accepted the request, or it did not. This design records `unknown` and stops automatic resubmission until reconciliation supplies evidence or an operator authorizes a new attempt. That choice sacrifices some unattended progress to reduce duplicate dispatch risk. It does not claim exactly-once delivery across a network boundary; the exactly-once mindset applies to preserving one business obligation and making every departure from it explainable.

## 2. How should SaaS teams compare an SMS alerts API for report notifications?

Use the following table as an architecture comparison, then ask each candidate integration to demonstrate the evidence required by the selected design. The entries describe proposed application behavior, rather than capabilities attributed to a particular service.

| Proposed design | Evidence the application retains | Failure boundary | Decision for this report workflow |
| --- | --- | --- | --- |
| Dispatch inside the report request | Request context and whatever results are saved before the request ends | Report generation and notification execution share a request lifetime | Reject as the default; require a separate durable obligation |
| Commit an obligation and dispatch through a worker | Report revision, recipient snapshot, attempt history, and explicit unresolved outcomes | External acceptance remains separate from application state | Adopt, provided the journal and worker claims are durable |
| Send an SMS notification without the attachment obligation | Evidence about the SMS notification only | The attachment requirement has no completion record | Reject for this job; valid when the job is only a text alert |

For a US/EU SaaS evaluation, request a concrete demonstration using the same sample obligation and failure sequence for every candidate: normal acceptance, an interrupted response, repeated completion evidence, and a destination that cannot be used. Record what the application can actually correlate, what needs manual investigation, and which destination or geographic restrictions require further documentation. Do not extrapolate regional support from a successful test of one destination.

Cost belongs in that evaluation, but after correctness. Account for engineering effort spent resolving unknown outcomes, maintaining recipient policy, and investigating disputes as explicit workload assumptions; do not disguise them as measured savings. A simpler integration may be easier for the team to maintain, while one that exposes more useful evidence may reduce the scope of manual reconciliation. The decision depends on the evidence available for this particular workflow.

If Twilio, Vonage, and Plivo appear on the procurement shortlist, apply this same evidence exercise to each. Their appearance on a shortlist does not establish attachment support, destination eligibility, or equivalent delivery semantics; require primary documentation and observed integration results before recording any such capability. This note makes no product ranking.

Evidence precedes selection.

## 3. Implement the critical state transition in Go

Implement the state machine locally before connecting a transport. The following complete program is an executable specification: save it as `main.go` and run it with `go run main.go`. Its in-memory journal and stub submission function deliberately model only the transition boundary. They provide no persistence, network dispatch, email composition, or delivery confirmation.

The example records an ambiguous first attempt, processes later acceptance evidence, and then processes that same evidence again. The repeated event must leave the audit length unchanged. Submission after acceptance must also leave the attempt count unchanged. These checks exercise business idempotency without pretending that the application controls the recipient's inbox.

```go
package main

import (
	"errors"
	"fmt"
)

type State string

const (
	Pending  State = "pending"
	Unknown  State = "unknown"
	Accepted State = "accepted"
)

type Entry struct {
	Kind string
	Ref  string
}

type Obligation struct {
	Key      string
	Report   string
	State    State
	Attempts int
	Audit    []Entry
	Seen     map[string]bool
}

type Receipt struct {
	Accepted bool
	Ref      string
}

type Submit func(key, report string) (Receipt, error)

func attempt(o *Obligation, submit Submit) {
	if o.State != Pending {
		return
	}
	o.Attempts++
	o.Audit = append(o.Audit, Entry{"attempt-started", o.Key})
	// Conservatively retain uncertainty before crossing the boundary.
	o.State = Unknown
	r, err := submit(o.Key, o.Report)
	if err != nil || !r.Accepted || r.Ref == "" {
		o.Audit = append(o.Audit, Entry{"outcome-unknown", o.Key})
		return
	}
	o.State = Accepted
	o.Audit = append(o.Audit, Entry{"transport-accepted", r.Ref})
}

func reconcile(o *Obligation, eventID, key, reference string) error {
	if eventID == "" || reference == "" || key != o.Key {
		return errors.New("invalid or unrelated acceptance evidence")
	}
	if o.Seen[eventID] {
		return nil
	}
	if o.State != Unknown && o.State != Accepted {
		return errors.New("acceptance evidence requires a prior attempt")
	}
	if o.Seen == nil {
		o.Seen = make(map[string]bool)
	}
	o.Seen[eventID] = true
	o.State = Accepted
	o.Audit = append(o.Audit, Entry{"acceptance-reconciled", reference})
	return nil
}

func main() {
	o := Obligation{
		Key:    "marketplace-42/settlement-17/report-r3/email/seller-8",
		Report: "settlement-17-report-r3.pdf",
		State:  Pending,
	}
	stub := func(key, report string) (Receipt, error) {
		return Receipt{}, errors.New("submission outcome unavailable")
	}
	attempt(&o, stub)
	if o.State != Unknown || o.Attempts != 1 {
		panic("ambiguous submission must remain unresolved")
	}
	if err := reconcile(&o, "event-1", o.Key, "transport-ref-1"); err != nil {
		panic(err)
	}
	entries := len(o.Audit)
	if err := reconcile(&o, "event-1", o.Key, "transport-ref-1"); err != nil {
		panic(err)
	}
	attempt(&o, stub)
	if len(o.Audit) != entries || o.Attempts != 1 {
		panic("repeated processing changed the obligation")
	}
	fmt.Println(o.State, o.Attempts, len(o.Audit))
}
```

The expected result is `accepted 1 3`. It means the model retained 3 audit entries around 1 submission attempt. It says nothing about delivery.

In a deployed implementation, place obligation creation in the same durable transaction as the report-ready transition. Make the obligation key unique, persist the attempt-started event before dispatch, and commit reconciliation state together with its deduplication record. Serialize worker ownership of an obligation; the sample's ordinary map is suitable for this single-threaded demonstration, not concurrent workers. On recovery, an attempt that started without a conclusive result must enter reconciliation rather than returning to pending.

The reconciliation function assumes its caller has already authenticated and correlated the evidence.

A matching string is not authentication.

Preserve the original evidence reference and a bounded, access-controlled record of the validation decision; avoid putting report contents or recipient contact details into general application logs. A production adapter must also distinguish authoritative rejection from uncertainty, because a confirmed rejection can support a different retry decision from an interrupted response.

## 4. Validate the attachment and the operational evidence

Extend the runnable example's checks at the adapter boundary. Use a generated fixture report whose revision and digest are known, verify that the composed email actually contains the intended attachment, and check that the recipient snapshot belongs to the obligation. A successful state-machine test cannot detect the wrong file attached to the right email. For a marketplace settlement report, that distinction is part of correctness.

Then exercise recovery: terminate a worker after its attempt record is committed, interrupt the transport response, and replay an authenticated reconciliation event twice. The acceptance criterion is that the obligation remains explainable and no extra attempt is created without an explicit decision. Do not promise that every uncertainty can be resolved automatically. Document who can authorize a resend and what evidence that authorization must retain.

For deployment, start with a controlled recipient fixture and inspect the composed attachment before enabling a larger workload. Observe unresolved-obligation age, attempt count, and reconciliation backlog separately from any delivery evidence the integration exposes. An acceptance counter alone cannot answer a seller's question about a missing report.

Authentication and compliance impose additional boundaries. DMARC evaluates alignment involving the message's author domain and authenticated identifiers; it does not certify the business correctness of an attachment or establish that a recipient read it. Its policy and reporting mechanisms are relevant to the email path, but an application audit record remains necessary for the report obligation. The WebOTP API concerns retrieving a one-time password from a specially formatted SMS; it supplies no evidence that this email attachment arrived.

Neither reference establishes permission to contact a particular recipient, a lawful retention period, or compliance across US and EU jurisdictions. Define those requirements with the responsible legal and security teams, then translate the approved policy into recipient eligibility checks, journal access controls, and retention rules. An audit trail is evidence of application behavior, not a compliance certificate.

## 5. Record the rejected option and its valid use case

Reject a notification-only design for this workflow because its completion evidence cannot discharge the attachment obligation. A delivered SMS about a settlement report would still leave the report email unresolved. Treating the two channels as interchangeable would erase the exact distinction a reconciliation process needs to preserve.

That simpler design remains appropriate when the actual requirement is a text alert and no attachment is owed. In that case, define the obligation around the text notification and evaluate its delivery evidence directly.

The journal-backed approach has limitations: it requires durable storage, worker coordination, and an operational owner for unresolved outcomes. The trade-off is explicit control in exchange for additional application responsibility. It is not suitable for a disposable demonstration with no recovery requirement; an inline send can serve that narrower purpose. The in-memory example also does not support crash recovery and must not be deployed as the durable journal.

For marketplace report dispatch, adopt the journal-backed worker, keep unresolved outcomes visible, and require an explicit reconciliation decision before repeating an uncertain submission. The durable result should be an answer to three questions: which report was owed, which attempt crossed the boundary, and what evidence supports the current state.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- MDN, WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
