# Compatibility promises

This document is about what users can expect when upgrading gor itself. It does not arrange the application's own upgrades, and it promises nothing about unpublished development versions.

Promises take effect from publicly released versions. Preview versions are for trying out and giving feedback, not an excuse to skip change notes.

## What v0 covers

v0 is the stage that completes the product and states its boundaries clearly; it is not a pretend-stable v1.

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

Every public release has release notes. Whenever there are incompatible changes, the notes must have a "Breaking changes and migration" section:

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

There are no public version tags yet, so no compatibility promises are in effect. Not all pre-release requirements are complete; the README presents single-process as the usable scope and describes multi-node failure detection as direct probing with death voting, while the reliability limits are stated in this document's "What can already be relied on" section.
