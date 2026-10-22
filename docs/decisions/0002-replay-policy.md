# ADR 0002 — Which requests may be replayed after a 401

**Status:** Accepted
**Date:** 2026-10-22
**Relates to:** DEVELOPMENT_PLAN task T4.5

## Context

A 401 from Salesforce means the request was rejected because the access
token was missing, expired, or invalidated. The tool used to treat that
as a hard failure: `SalesforceClient._handle_response` raised
`SalesforceAuthError` and the request was never sent again.

That is unnecessarily brittle. The token store's expiry detection is an
*observation window* rather than a real expiry, because the SF CLI
reports no expiry and the access token is opaque. So there is a window —
"the token was refreshed thirty minutes ago" — in which Salesforce can
have invalidated the token anyway. Recovering from that needs a refresh
followed by a replay.

The difficulty is that "replay the request" is not a single safe
operation. It is safe for a read and dangerous for an action.

The concrete hazards in this codebase:

- `POST /wave/dataflowjobs` starts a dataflow. Replaying it could start
  the dataflow twice.
- `DELETE /wave/datasets/{id}` deletes a dataset. Replaying it could act
  on an id the platform has since recycled onto a different asset.
- `PATCH /sobjects/InsightsExternalData/{id}` with `{"Action":
  "Process"}` processes a Data Manager job. The path names one object,
  but the action is a side effect rather than an assignment.

## Options considered

### A. Replay reads always; replays an identified asset; never replays an action (chosen)

- `GET` and `HEAD` are always safe to repeat. They have no side effect.
- `PUT` and `DELETE` are safe to repeat when the path names one specific
  asset, because a second attempt cannot land on a different object.
- `POST` and `PATCH` are never replayed. Both are used for actions.

### B. Replay everything exactly once

The argument for this is that a 401 means the platform rejected the
request *before* doing any work, so the first attempt had no side
effect and the second cannot duplicate one. A `POST` that returned 401
never started the dataflow.

This is a reasonable reading of the protocol, and it is the one the
retry machinery of most HTTP clients takes. It was rejected because it
rests on an assumption the tool cannot verify: that every 401 came from
Salesforce. A 401 can also come from a proxy or a load balancer in
front of the org, and in that case the origin may well have processed
the request before the proxy rejected it. The cost of being wrong in one
direction is a duplicated dataflow; the cost of being wrong in the other
is a 401 that the user has to retry by hand.

### C. Replay reads only

The most conservative option. Every write fails on a 401, even one that
names a specific asset and would have been perfectly safe to repeat.

Rejected because it makes a recoverable session expiry look like a
fatal error in more cases than necessary, for no safety gain on the
operations that actually matter.

## Decision

Use option A.

The "explicit asset id" test is deliberately mechanical rather than a
judgement call at each call site, so that the policy cannot drift:

> The last path segment of the URL is 15 or 18 alphanumeric characters
> and contains at least one digit.

Fifteen and eighteen are the lengths of a Salesforce id. The digit
requirement is what separates an id from a collection name — a
15-character segment with no digits in it is a resource name, not an
id. Salesforce ids always start with a three-character key prefix
(`0Fb`, `0Fc`, `00l`), so in practice they always contain digits.

The rule is implemented in `asftool/core/replay.py` and is exercised by
`tests/unit/test_replay_policy.py`.

### `PATCH` is not replayed

`PATCH` is excluded even though the one path that uses it names an
asset. "Process this job" is an action, not an assignment: repeating it
would ask the platform to process the same job twice. The rule is
about what the request *does*, not only about what it addresses.

## Consequences

- A 401 on any read recovers silently: the token is refreshed and the
  request is repeated.
- A 401 on a dataflow start, a dashboard creation, or a Data Manager
  action still fails, but the token *has* been refreshed, so the user's
  next command starts from a working session. The error says the
  request was not retried and why.
- Concurrent requests that all get a 401 share a single refresh. Without
  that, a field scan of two hundred assets would trigger two hundred
  refreshes.
- The refresher is a callback injected into `SalesforceClient`, not an
  import. `core/auth/` already imports from `core/`, so a direct import
  in `core/client.py` would be circular.
- `SFCLIAuthService.get_access_token` could not serve as the refresher:
  it returns the *stored* token whenever the store considers it
  unexpired, which is precisely the token that just failed.
  `refresh_access_token` forces a refresh regardless.
- A refresh that fails is reported, and the request is not replayed with
  the stale token. Retrying with a token that is known to be bad is how
  a loop starts.
