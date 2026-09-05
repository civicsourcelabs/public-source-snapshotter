# Luna-only model and cost policy

## Effective scope

The root Codex model remains the user's choice. All delegated agents use
`gpt-5.6-luna`; project defaults and default/worker/explorer custom agents pin
Luna with medium reasoning and four concurrent children. Do not use an
inherited non-Luna full-history fork. This is layered configuration and a
repository contract, not an immutable platform allowlist.

Run `python3 scripts/verify_openai_policy.py --self-test` before submitting
changes. CI repeats the offline check. Tests/docs/fixtures may describe denied
models; executable transport outside the approved gateway is rejected.
Static scanning detects ordinary SDK/HTTP/model bypasses, not arbitrary
obfuscated code or agent edits to the scanner. Provider restrictions remain
the final boundary. No keys are needed for this check.

## Owner-only platform setup

1. Create/select a dedicated API Project for these workloads.
2. Configure Model Usage with `mode=allow_list` and exactly
   `model_ids=["gpt-5.6-luna"]`. Prefer an allowlist so future models are denied.
3. Create a project-scoped service account/key; do not give the executor an
   admin key or another unrestricted project key.
4. Replace existing runtime secret values through their current secret store
   (applicable execution environments). Never commit key values.
5. After validating the replacement, the Owner can revoke old keys. This
   implementation does not create, retrieve, rotate, delete or revoke keys.
6. Set project spend alerts/rate controls and an explicit operating budget.
   Do not assume a dashboard alert is a synchronous hard spending cutoff.
   Runtime call/token/concurrency limits are separate safeguards.

Do not verify a deny rule by sending an Astra/Sol/Terra request: a misconfigured
project could execute and charge for it. Inspect/export the project's model
permission policy and verify the project/key association instead. An offline
export is a snapshot, not proof of current key routing.

## Official references

- [Codex subagents](https://learn.chatgpt.com/docs/agent-configuration/subagents)
- [Codex configuration](https://learn.chatgpt.com/docs/config-file/config-reference)
- [Luna model](https://developers.openai.com/api/docs/models/gpt-5.6-luna)
- [Project model permissions](https://developers.openai.com/api/reference/python/resources/admin/subresources/organization/subresources/projects/subresources/model_permissions/methods/update)

No OpenAI application transport is currently authorized in this repository. The offline guard rejects newly introduced SDK/HTTP calls; do not add another client to bypass the policy.
