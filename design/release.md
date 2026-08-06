# Release

This document specifies how maintainers release gor. It is deliberately a short list: with a single maintainer today, leaving repetitive but rare judgment calls to a person is more reliable than building a release system.

## The conclusion

Each version has exactly one user-facing release note, written in that version's GitHub Release. The repository keeps no second CHANGELOG and assembles no notes from PRs automatically.

The `release-note` code block in `.github/PULL_REQUEST_TEMPLATE.md` is the raw material of release notes. Its only consumer is the maintainer at release time: hand-read the blocks in the list of merged PRs, then write that version's release note. An author stating user impact while the change is fresh is more accurate than re-reading all changes after a delay.

The raw material is not a second release note. The maintainer may merge, rewrite, or exclude content that does not belong to this version; only the final GitHub Release is in effect externally. Do not add parsers, workflows, or generated files for these blocks: under single-person maintenance, version boundaries and wording still need human judgment; automation saves no work and only adds process to maintain.

The release note must at least include:

- Additions, fixes, or limitations users can see.
- Breaking changes and migration; "none" when there are none.
- Known limitations, especially those related to cluster reliability.
- For v1 and later major versions, a migration entry for users of the previous major version.

## Version numbers

Version tags use `vMAJOR.MINOR.PATCH`. Previews may use a suffix like `v0.1.0-rc.1`; a preview does not replace a proper release and does not change the compatibility rules of proper versions.

| Scope | May contain | Must not do |
| --- | --- | --- |
| v0 patch | Bug fixes, doc fixes, and small changes that do not alter documented usage. | Silently change documented usage or require migration. |
| v0 minor | New capabilities, or incompatible changes explained in the release note. | Disguise breaking changes as patches. |
| v1 patch | Bug fixes and doc fixes. | Change promised public usage or require relocating data gor manages. |
| v1 minor | Backward-compatible new capabilities. | Delete or redefine promised public usage, or make existing gor-managed data unusable. |
| New major | Public changes that cannot be compatible. | Keep the old module path to pretend there is no migration cost. |

v0's minor version is a compatibility boundary, a discipline gor imposes on itself beyond Go's looser v0 requirements. A patch may still fix a bug and change a previously wrong result, but the release note must say so.

The transition from v0 to `v1.0.0` may include one documented migration. Only after `v1.0.0` do the rules that v1 requires no migration take effect.

Go's module major-version rules make v1 a valuable stability promise: v0 and v1 use the repository root path, so users upgrading to v1 do not change import paths for a major-version suffix. From v2 on, module declarations and import paths must carry `/v2`, `/v3`, and so on. That forces every user to change source code, so v2 and beyond only happen when the v1 promise truly cannot be kept.

## The first public release

`v0.1.0` can only be released after all of [ROADMAP.md](../ROADMAP.md)'s "required before release" items are done. Completion is judged by the roadmap's explicit status, not by "the code looks close enough".

Every "required before release" item except the last one is done: English documentation, public API doc comments, the error and cancellation contract, the root runtime shutdown contract, deactivation reasons and the background error sink, the example application, the performance and cross-node forwarding baselines, and observability. What remains is the "Versioning and release" item itself: the release process this document describes, including the install-and-upgrade check in release-sequence step 3.

Multi-node is still a preview capability and partitions can misjudge healthy nodes, so the first public release must not be treated as settled.

## The bar for v1.0.0

Each of the following must be true before `v1.0.0` can be released:

1. Roadmap steps 1 through 6c and every "required before release" item are marked done.
2. `make ci` succeeds on the candidate commit.
3. State written by the latest public v0 and generated call artifacts still work in a clean user project upgraded to the candidate version; if not, the release note gives a verified migration path.
4. The candidate's public usage and limitations are aligned with [docs/compatibility.md](../docs/compatibility.md); no known core gap hidden behind a "Gap" section.
5. The `v1.0.0` release note states the outcome of upgrading from the latest v0 and lists every item requiring user action.

No extra thresholds like "wait until mature", "wait until enough users", or a fixed date. When all five above hold, release; while any one does not, stay in v0. This way v1 means a verifiable compatibility promise, not project age.

## Release sequence

1. Hand-read the merged PRs' `release-note` blocks, list what this release includes, and choose the version number per the previous section. Breaking changes go only into v0 minors or new major versions.
2. Update the affected product promises, designs, and the roadmap; keep or add a "Gap" section where a limitation has not gone away.
3. Run `make ci` on the candidate commit. For the first public release, a major version, artifact changes, or persistence-related changes, also run an install-and-upgrade check in a clean user project.
4. Inspect the to-be-released commit, version tag, and working tree. Only verified content enters the release.
5. Hand-write the GitHub Release note from these blocks as raw material, then create the version tag and the Release. The library produces no standalone service binary, so no fake download packages.
6. After the release, install the exact version in a clean user project and run one minimal call. On failure, retract or mark that Release first; do not ship a patch to cover up an uninstallable version.

Steps 1, 2, 4, and 5 need maintainer judgment and stay manual. Step 3 already has `make ci`, the only release gate worth automating permanently. Step 6 is a low-frequency external availability check; scripts, workflows, or auto-created Releases cost more maintenance than they earn.

## Gap

There are no version tags today, and `v0.1.0`'s roadmap thresholds are not yet met — the "Versioning and release" item is the only one left. The `release-note` block in the PR template stays; at release time it is still read and organized by hand per this document.

The install half of release-sequence step 3 was run on the master pseudo-version `v0.0.0-20260806020541-f69fa66383f8` in a clean user module outside the repository. What passed: `go get github.com/suraciii/gor@<sha>` resolves through the proxy; the generator emits artifacts that compile in the user module; a minimal entity flow works end to end — register, call (`Deposit` / `Balance`), close the runtime, reopen the store, and the state survives the restart.

Two gaps in the documented flow were found, both on the user's first steps, not in the library runtime:

1. `go get` of the module alone does not make the documented `go run github.com/suraciii/gor/cmd/gorgen -pkg ./domain` work. The run fails with a missing go.sum entry for `golang.org/x/tools/go/packages`, a dependency of the generator only. The user must run the extra `go get github.com/suraciii/gor/internal/codegen@<sha>` that the error itself suggests. [codegen.md](codegen.md)'s Invocation section does not mention this step.
2. The generator's default output, the entity package's `internal/gorgen` subpackage, cannot be imported by the application's startup code: Go's `internal` rule only allows import from inside the entity package tree, while `Install` is documented to be called where the runtime is created, outside it. The documented flow therefore does not compile with default flags. The working invocation passes `-out` to place the generated package outside the entity package (for example `-out ./gorgen`); the example application's generated package sits outside `domain` for the same reason, but neither [codegen.md](codegen.md) nor [docs/programming-model.md](../docs/programming-model.md) tells the user so. Whether to change the default output location or to document `-out` in the user-visible flow is an open decision; either way, the first public release's install check must pass as documented.

The upgrade half of the step-3 check could not be run: there are no version tags, so there is no previous version to upgrade from. It becomes meaningful only after `v0.1.0`, when release-sequence step 6 also becomes runnable for the first time.
