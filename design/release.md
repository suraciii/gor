# Release

This document specifies how maintainers release gor. It is deliberately a short list: with a single maintainer today, leaving repetitive but rare judgment calls to a person is more reliable than building a release system.

## The conclusion

Each announced release has exactly one user-facing release note, written in that version's GitHub Release. 0.0.x tags are not announced releases: they get no note and no GitHub Release (see "0.0.x" below). The repository keeps no second CHANGELOG and assembles no notes from PRs automatically.

The `release-note` code block in `.github/PULL_REQUEST_TEMPLATE.md` is the raw material of release notes. Its only consumer is the maintainer at release time: hand-read the blocks in the list of merged PRs, then write that version's release note. An author stating user impact while the change is fresh is more accurate than re-reading all changes after a delay.

The raw material is not a second release note. The maintainer may merge, rewrite, or exclude content that does not belong to this version; only the final GitHub Release is in effect externally. Do not add parsers, workflows, or generated files for these blocks: under single-person maintenance, version boundaries and wording still need human judgment; automation saves no work and only adds process to maintain.

The release note must at least include:

- Additions, fixes, or limitations users can see.
- Breaking changes and migration; "none" when there are none.
- Known limitations, especially those related to cluster reliability.
- For v1 and later major versions, a migration entry for users of the previous major version.

## Planning

This section is the front half the rest of the document assumes: when a release is cut and which issues belong to it. Version numbers and the release sequence describe a release already decided to happen.

A milestone is the set of issues committed to one tag. It is the only planning object: there is no release calendar, no freeze window, no per-release branch. An issue either carries a milestone (committed to that tag) or carries none (backlog).

### When a release is cut

A **0.0.x** tag (publicly visible, not announced — see "0.0.x") may be cut when its milestone has zero open items and `make ci` is green on the candidate commit. GitHub counts open issues and open pull requests against a milestone; both must be closed. No date, no issue count, no feature threshold: the batch committed to the milestone is done. This is the default release kind while gor is pre-announcement.

An **announced release** (v0.1.0 and later) is cut by maintainer judgment, not by a checklist — see "The bar for v1.0.0" and the 0.0.x note that readiness is complete and announcing is a choice. The 0.0.x chain leads up to it; the maintainer decides when to stop tagging 0.0.x and announce.

Closing a milestone freezes its scope as a completed set; it is not the same act as pushing the tag, which stays the manual step in "Release sequence." The two may be separated — v0.0.1's milestone was closed as a scope marker, and its tag was pushed afterward.

### Which issue goes in which milestone

The current 0.0.x is the lowest-numbered open milestone. Route a new issue by three questions, in order.

1. **Is it required before the first announced release?** Required means a spec already written in `design/` but not implemented, where that spec is part of what the release promises (a hole in the project's stated differentiator), or a defect in something the release promises. If required, the issue belongs somewhere in the 0.0.x chain ahead of v0.1.0. If not, go to question 3.
2. **Is it cut-blocking?** A defect a user hits with ordinary use is cut-blocking: route it to the current 0.0.x. A written-but-unimplemented spec, or a defect that needs unusual input to trigger, is not cut-blocking: route it to the next 0.0.x (create one if none exists). Once in a milestone, an issue stays there until it is done or is moved at cut time.
3. **Otherwise it is backlog.** An issue with no milestone is recorded work not committed to any tag — future direction, environment debt with no user impact, or a gap explicitly deferred. No milestone is a legitimate, intended state, not a mistake and not a queue to drain before a release.

The one fork not decidable from the issue alone is "ordinary use" versus "unusual input" in question 2. That is a judgment about how likely a user is to hit the defect. Worked example: a codegen import collision triggered by two method-signature packages sharing a name (common — cut-blocking, current 0.0.x) versus one triggered only by an entity package literally named `context` (rare — next 0.0.x). The agent proposes a placement; the maintainer confirms that single fork. Seeding a milestone's initial batch, deciding to announce, and judging an issue misrouted when its work cannot wait are also maintainer calls; routing, straggler-moving, and the zero-open cut check are mechanical.

### Merge order on the linear trunk

The trunk is linear — one master, no per-release branch — so a tag holds exactly what the trunk held at cut time. That is the linear trunk's fact, not an option: nothing keeps a later milestone's work out of an earlier tag except the order of merges.

Therefore finished work committed to a later milestone merges only after the earlier milestone is cut; a done PR sits open until then. Merging it earlier would put the later milestone's work into the earlier tag, which then carries scope its milestone never committed to.

If the wait is unacceptable, the original routing was wrong — the issue does not belong in the milestone it was given. The remedy is to change the issue's milestone, never the tag's content. Judging the routing wrong is a maintainer call, not a mechanical step.

### Unfinished work at cut time

When a 0.0.x is otherwise closeable but an issue in it is not done, move the unfinished issue to the next 0.0.x, then close and cut. A milestone is a completed set, not a promise; reaching zero-open by moving a straggler is correct.

One exception: if the only open item is the milestone's reason for existing — the work its description was created to deliver — the cut waits for it. An empty milestone is not cut; if that work cannot finish, restructuring the plan is a maintainer decision, not an automatic move.

The precedent: a narrow codegen defect found while fixing the common one was routed to the next 0.0.x rather than expanding the current milestone, so the current tag cut on its committed batch.

### Creating and closing milestones

Create a milestone when a batch of issues is committed to it; create the next 0.0.x when the current is seeded or when a straggler must move and no next exists. Do not pre-create empty milestones beyond what is planned. Close a 0.0.x when it is cut. The first 0.0.x may be created already-closed to mark what master was at the moment versioning started; v0.0.1 is that case.

## Version numbers

Version tags use `vMAJOR.MINOR.PATCH`. Previews may use a suffix like `v1.0.0-rc.1`; a preview does not replace a proper release and does not change the compatibility rules of proper versions.

| Scope | May contain | Must not do |
| --- | --- | --- |
| v0 patch | Bug fixes, doc fixes, and small changes that do not alter documented usage. | Silently change documented usage or require migration. |
| v0 minor | New capabilities, or incompatible changes explained in the release note. | Disguise breaking changes as patches. |
| v1 patch | Bug fixes and doc fixes. | Change promised public usage or require relocating data gor manages. |
| v1 minor | Backward-compatible new capabilities. | Delete or redefine promised public usage, or make existing gor-managed data unusable. |
| New major | Public changes that cannot be compatible. | Keep the old module path to pretend there is no migration cost. |

v0's minor version is a compatibility boundary, a discipline gor imposes on itself beyond Go's looser v0 requirements. A patch may still fix a bug and change a previously wrong result, but the release note must say so.

The 0.0 minor (0.0.x) is the band before that discipline takes effect. gor tags it so the work is publicly visible and `go get`-able, but it is not an announced release: no GitHub Release is created and no release note is assembled. Go's v0 promises nothing here, and gor adds only the two items named in [docs/compatibility.md](../docs/compatibility.md)'s "0.0.x" section. Anything else may change between 0.0.x tags, stated in the PR's `release-note` block rather than silently. The patch/minor discipline in the table above starts from 0.1.0.

The transition from v0 to `v1.0.0` may include one documented migration. Only after `v1.0.0` do the rules that v1 requires no migration take effect.

Go's module major-version rules make v1 a valuable stability promise: v0 and v1 use the repository root path, so users upgrading to v1 do not change import paths for a major-version suffix. From v2 on, module declarations and import paths must carry `/v2`, `/v3`, and so on. That forces every user to change source code, so v2 and beyond only happen when the v1 promise truly cannot be kept.

## 0.0.x

gor is in the 0.0.x band. A 0.0.x tag is publicly visible — the repository is public, so a tag is `go get`-able the moment it is pushed and the module proxy caches it; a tag is not access-controlled — but it is not an announced release. No GitHub Release is created and no release note is assembled; the version number is the only signal, and in Go's terms v0 promises nothing. What gor holds stable even here is the narrow set in [docs/compatibility.md](../docs/compatibility.md)'s "0.0.x" section; everything else may change between tags.

The 0.0.x release-note question has a zero-maintenance answer: none is written. The `release-note` blocks in merged PRs still accumulate as raw material; their only consumer is the maintainer writing the first announced release's note. Nothing is assembled, published, or kept in sync per 0.0.x tag.

The readiness work for an announced release is already complete. Every [ROADMAP.md](../ROADMAP.md) "required" item is done — English documentation, public API doc comments, the error and cancellation contract, the root runtime shutdown contract, deactivation reasons and the background error sink, the example application, observability, and the performance baseline — and `make ci` passes. The benchmark failure that had blocked the baseline is fixed (`cluster.New` honors the six probe-parameter defaults from [design/cluster.md](cluster.md), `make bench` passes on a real-disk path, the forwarding baseline re-verified on 2026-08-06), and the cluster startup snippet in [docs/programming-model.md](../docs/programming-model.md) runs verbatim in a clean module. The reason gor is at 0.0.x and not announced is the maintainer's judgment that it is not time, not a missing technical gate. Inventing a new checklist to "earn" an announced release would be dishonest; when the maintainer decides to announce, that decision is the gate.

Multi-node is still a preview capability and partitions can misjudge healthy nodes, so no release — 0.0.x or announced — should be treated as settled.

## The bar for v1.0.0

Each of the following must be true before `v1.0.0` can be released:

1. Roadmap steps 1 through 6c and every "required before release" item are marked done.
2. `make ci` succeeds on the candidate commit.
3. State written by the latest public v0 and generated call artifacts still work in a clean user project upgraded to the candidate version; if not, the release note gives a verified migration path.
4. The candidate's public usage and limitations are aligned with [docs/compatibility.md](../docs/compatibility.md); no known core gap hidden behind a "Gap" section.
5. The `v1.0.0` release note states the outcome of upgrading from the latest v0 and lists every item requiring user action.

No extra thresholds like "wait until mature", "wait until enough users", or a fixed date. When all five above hold, release; while any one does not, stay in v0. This way v1 means a verifiable compatibility promise, not project age.

## Release sequence

For a 0.0.x tag, only the verification core runs: `make ci` (step 3), the install-and-upgrade check when the change touches generated artifacts or persistence (step 3), keep docs and Gap honest (step 2), inspect the commit and working tree (step 4), then create the tag (step 5, tag only). No release note is assembled and no GitHub Release is created. The full six steps are for announced releases.

1. Hand-read the merged PRs' `release-note` blocks, list what this release includes, and choose the version number per the previous section. Breaking changes go only into v0 minors or new major versions.
2. Update the affected product promises, designs, and the roadmap; keep or add a "Gap" section where a limitation has not gone away.
3. Run `make ci` on the candidate commit. For the first announced release, a major version, artifact changes, or persistence-related changes, also run an install-and-upgrade check in a clean user project.
4. Inspect the to-be-released commit, version tag, and working tree. Only verified content enters the release.
5. Hand-write the GitHub Release note from these blocks as raw material, then create the version tag and the Release. The library produces no standalone service binary, so no fake download packages.
6. After the release, install the exact version in a clean user project and run one minimal call. On failure, retract or mark that Release first; do not ship a patch to cover up an uninstallable version.

Steps 1, 2, 4, and 5 need maintainer judgment and stay manual. Step 3 already has `make ci`, the only release gate worth automating permanently. Step 6 is a low-frequency external availability check; scripts, workflows, or auto-created Releases cost more maintenance than they earn.

## Gap

0.0.x tags are cut under the Planning rules: each is annotated, and no GitHub Release is created for any of them. The readiness work for an announced release is complete; staying in 0.0.x rather than announcing is a choice, not a missing gate.
