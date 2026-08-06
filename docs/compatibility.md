# Compatibility promises

This document is about what users can expect when upgrading gor itself. It does not arrange the application's own upgrades, and it promises nothing about unpublished development versions.

The repository is public, so a version tag is `go get`-able the moment it is pushed, and the module proxy caches it; a tag is not access-controlled. What users may rely on is told by the version number, not by who can fetch it. Go's v0 makes no compatibility promise; the sections below state where gor adds discipline on top of that, and where it deliberately does not.

## 0.0.x

0.0.x is the band before gor's v0 discipline takes effect. The tags are public, but they are not announced — their only signal is the version number itself, which in Go's terms promises nothing.

Go's v0 already promises nothing here, and gor keeps stable only two things users cannot avoid depending on, even at 0.0.x:

- The stable error codes that distinguish one failure from another. A later 0.0.x does not rename or remove a code a user may branch on; it may add new ones.
- State gor manages. A later 0.0.x still reads state an earlier 0.0.x wrote. (Recovering confirmed state after restarting the same version is a correctness promise, not a compatibility one; it holds in every version.)

Everything else may change between 0.0.x tags — the categories a v0 minor version may change, listed under "What v0 covers", may change here even patch to patch. If a change has to touch one of the two items above, the PR that makes it states the impact; it is not done silently.

## What v0 covers

From 0.1.0 on, v0 is the stage that completes the product and states its boundaries clearly; it is not a pretend-stable v1.

Within one v0 minor version, patch versions do not actively break documented usage. Bug fixes may make previously wrong results, timings, or errors disappear; such user-visible fixes are written into release notes too.

When moving to the next v0 minor version, gor reserves the right to change:

- how the runtime is wired up and entities are called.
- the automatically generated call artifacts.
- options, error details, and resource usage.
- routing, fault handling, and recovery behavior in a cluster.
- undocumented behavior, performance numbers, and internal timing.

Planned generated-artifact changes fall in this category. They do not sneak in through patch versions. Every incompatible v0 minor version must state in its release notes the impact, who is affected, and how to migrate. When no automatic migration exists, say so plainly.

Production should pin to a specific v0 minor version. Moving to the next minor version is a scheduled change, not routine patching.

## What can already be relied on

v0's usable scope is single-process. For the published single-process capabilities, users can rely on these basic semantics:

- calls on the same identity execute in order.
- state confirmed successfully survives a process restart.
- documented scheduled delivery, overload, and failure outcomes behave as described.

These are product promises, not promises about implementation shape, throughput numbers, or exact execution instants.

Multi-node is still a preview capability. Current failure detection is based on direct probing of neighbors and death votes with expiry, but a network partition can mistake healthy nodes for failed ones, even stopping every node from serving; recovery needs a new generation. So during v0, multi-node availability, failure-detection accuracy, and upgrade experience are not stable guarantees. Related behavior may be adjusted or withdrawn in a new v0 minor version.

The application remains responsible for evolving its own business state. gor does not understand business fields and will not convert old business data into new for the application. When the application changes method contracts or state meaning, it arranges compatible reads/writes or a downtime switch itself.

## How to know something is incompatible

Every announced release has a release note. (0.0.x tags do not; their changes are recorded in the PR `release-note` blocks and surface in the first announced release's note.) Whenever an announced release has incompatible changes, its note must have a "Breaking changes and migration" section:

- what user-visible behavior changed.
- which users are affected.
- what to do before and after upgrading; state clearly when it cannot be automated.

With no breaking changes, the section says "None". The version number lets users judge upgrade risk; the release notes let them judge the actual work; both are required.

## This is not application rolling upgrades

This compatibility is a contract between gor and its users. It does not change the constraints on applications upgrading within one cluster.

An application's nodes must still use mutually compatible method contracts and state formats. Incompatible application changes need a downtime release, or compatible reads/writes arranged by the application itself. This constraint is in [programming-model.md](programming-model.md); it is a different matter from gor itself moving from one version to another.

## What v1 means

Only v1 starts promising a stable public usage surface. From `v1.0.0`, across v1 patches and minor versions, documented wiring, call semantics, and data managed by gor must not require users to change code or hand-migrate data to keep working.

The move from v0 to `v1.0.0` may include one explicit migration; the release notes must describe it fully.

Bug fixes may still change error behavior, but the release notes must say so. Performance, undocumented behavior, and cluster conditions beyond the documented scope do not automatically become guaranteed because the version reaches v1.

If this v1 promise must ever be broken, gor will release a new major version with migration notes. v1 is not an indefinite support promise, and it does not mean gor will become an unboundedly scalable distributed system.

## Gap

There are no version tags yet. The readiness work for an announced release is complete (every ROADMAP "required" item is done); the maintainer has not tagged 0.0.1, and staying in 0.0.x is a choice, not a missing requirement. When 0.0.1 is tagged, the two 0.0.x promises above take effect; the broader v0 discipline and assembled release notes start at 0.1.0. The README presents single-process as the usable scope and describes multi-node failure detection as direct probing with death voting, while the reliability limits are stated in this document's "What can already be relied on" section.
