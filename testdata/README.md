# Conformance fixtures

All default SDK harnesses use [entry-manifest/manifest.json](entry-manifest/manifest.json)
for the current [v1 working draft](../spec/v1.md). Its schema version is independent
of the archive wire version. Run `just check` for native/installed SDK operation checks.

Each case names its archive, operation, expected returned bytes or error,
external object closure and verification scope. Artifact paths resolve inside
`entry-manifest`. Archive SHA-256 in this test contract pins fixture bytes; it
is not an archive wire field. Selective success does not establish full physical
validation. Physical validation of a thin archive checks pointer text and mapping;
closure validation also fetches all external objects.

Fixtures must identify the intended defect and regenerate unrelated enclosing
digests so they do not mask it. Host-specific restoration vectors are separate
from portable wire validity.

The Rust, Python, and Go SDK suites run the 292 retained operation cases through
their public APIs and compare normalized failure categories. Cases are kept
when they add a distinct validation scope, outcome, or boundary; repeated
metadata/read checks for the same defect are represented by one case. A
passing corpus is evidence for the listed cases, not exhaustive proof.

The current corpus covers entry/root/extent types and
bounds, namespace and required feature rules, pointer syntax, physical payload
mapping, mandatory checksums, and selective-versus-full validation. Ustar
vectors cover full-width octal fields, space-only zero
fields, invalid digits, and digits hidden after padding. Member coverage vectors
reject missing empty files, directories, and symlinks, as well as unindexed and
duplicate physical members; metadata validation alone accepts these physical
defects. PAX vectors cover unsigned size/owner syntax, signs, empty values,
whitespace, non-ASCII
digits, numeric overflow, exact int64 timestamp boundaries, leading/negative
zeros, and fractional digits beyond nanosecond precision. Selected invalid
physical PAX vectors are checked separately at metadata open, selective read,
and full validation.
Required-feature vectors distinguish legal unknown names from malformed
punctuation, non-ASCII names, and length boundaries. Cases may
include `schema_invalid_blocks`, a list of zero-based physical metadata block
indices expected to fail the [inline CDDL schema](../spec/v1.md#manifest-schema).
Other blocks must pass the schema, even when cross-field or physical invariants
make their archive invalid. This
annotation is test evidence, not a wire field or an SDK validation instruction.
`just check` runs SDK validation; it does not invoke a separate CDDL checker.
`frame_invalid_blocks` instead names compressed metadata payloads that must fail
the restricted frame profile before CBOR/schema validation. The annotations are
disjoint and consistent between operations on the same artifact.

Frame vectors cover absent content sizes, optional metadata/object checksums,
zero/nonzero dictionary IDs, actual dictionary dependencies, the reserved and
unused header bits, reserved blocks, concatenation/trailing/skippable data,
content-size/checksum mismatch, and the 16 MiB window boundary. Nested arrays
and maps exercise the default parser budget at its boundary and one level above.
`local_budget` labels a resource-policy case whose archive remains wire-valid;
it is descriptive evidence, not an SDK option.

The corpus is not exhaustive. Broader hostile/boundary, host-specific restoration
and cross-writer vectors remain required before freeze. SDK operation tests
cover cross-field, deterministic CBOR, source, and physical invariants.

Only the current entry-manifest fixtures are retained in this checkout.

Failure classes and validation dependency rules are defined by
[v1.md](../spec/v1.md#failure-classes). Concrete harness
categories remain stable, including `malformed-toc` and `tar-toc-mismatch` as
historical machine-readable category names for manifest structure/disagreement.
A lower local budget is `resource-limit`; it does not make wire data malformed.

Validate the corpus with the SDK suites:

```sh
just check
```
