# Findings

After two passes over the device shadow as an external user, the API friction that remains:

1. **Entity time: resolved.** The entity reads the runtime clock from its binder; the example writes report timestamps with `gor.Now(binder)`. Constructors no longer need to receive a clock, and tests control time.

2. **Entity-to-entity references: resolved.** An entity gets a typed reference to another entity from its own binder; the example notifies the workshop with `gor.Ref[Workshop](binder, workshopID)`. No factory closure capturing the runtime.

3. **Cross-entity consistency: still there, and deliberately not provided.** One report writes the device shadow, resets the offline timer, and notifies the workshop in sequence; when a later step fails, an earlier one may have succeeded. `gor` offers no cross-entity transactions, outbox, compensation, or unified retry state; the business must accept this consistency window, or put the state that must be atomic into one entity.

4. **Invisible scheduled-delivery failures: resolved, but only the visibility.** A failing scheduled method now comes out of the runtime's `OnError` sink; the example's two entry points both install it and verify, with a failing scheduled method, that it actually arrives. The runtime still does not retry; retrying, backing off, and alerting remain the application's call.

5. **`State[T].Get()` shared-value semantics: the API semantics stay; the docs now say it clearly.** A map or slice from `Get()` is the very instance the activation holds; after mutating it you must still call `Set()` for persistence; without `Set()`, eviction reverts to the old value in the store. This is deliberately kept semantics, not a friction step 5.5 removes.

6. **Missing lifecycle hooks: resolved.** Entities can implement `OnActivate` and `OnDeactivate`; the example's load generator waits on an `OnDeactivate` channel signal for real idle eviction, then calls the entity to confirm state is restored from the store; tests also use fake clocks to verify automatic eviction, hook calls, and reactivation. Hook errors still go through the unified `OnError` sink — see the new friction below.

New findings this round:

7. **Entities must keep their `Binder` themselves.** Both `gor.Now` and entity-to-entity `gor.Ref` need the `Binder`, so the entity stores it in a field for later methods and lifecycle hooks. This is the current design, not an implementation bug: it keeps entities from capturing runtime objects, but it is also the first rule a new user must remember.

8. **`OnError`'s source information too coarse: resolved.** Scheduled-delivery failures and `OnDeactivate` failures share one structured sink: the event gives the entity, the original error, and a closed source set — scheduled delivery carries the method name, deactivation failure carries the deactivation reason. The source set is closed; nothing outside the package can add to it, so a scheduled method that happens to be named `"OnDeactivate"` cannot be confused with the deactivation hook. It carries no schedule metadata or attempt counts, and reports no scan or claim failures; that information may be stale after claiming, the ETag is not an application decision, and the runtime has no retry model. The scheduled delivery canceled mid-shutdown is not reported either.

9. **`OnDeactivate` had no deactivation reason: resolved.** Deactivation distinguishes four application-actionable cases — idle, current node lost ownership, normal shutdown, instance untrusted — and the reason is fixed at the first transition out of the active state; no later event rewrites it. Applications can then choose to reclaim local resources, hand back node ownership, teardown before process exit, or alert on fault; reasons are not exposed one by one per internal implementation branch. The deactivation hook's work context has no deadline and is never canceled. An abrupt stop does not start teardown that has not begun; a graceful stop waits for teardown that has begun to return.
