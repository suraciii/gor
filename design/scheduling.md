# Scheduling

## Goal

Calls on the same entity are strictly serialized; calls on different entities are fully parallel.

## Implementation

One mailbox per entity: one goroutine reads from a channel and executes in a loop.

```go
type Box struct {
    in     chan *call
    done   chan struct{}
}

type call struct {
    fn    func(ctx context.Context) (any, error)
    reply chan result   // capacity 1
    ctx   context.Context
}

func (b *Box) run() {
    for c := range b.in {
        v, err := c.fn(c.ctx)
        c.reply <- result{v, err}
    }
    close(b.done)
}
```

Serialization falls out directly from the fact that only one goroutine runs the loop body. No locks, no custom scheduler.

**`reply` must have capacity 1.** After the caller times out it stops reading this channel ([runtime.md](runtime.md): a timeout means the caller is no longer waiting, not that the entity stops executing). With an unbuffered `reply`, the send would hang forever — not one call, but the whole mailbox loop; the entity would be dead from then on. Capacity 1 makes the send never block, and an unclaimed result is reclaimed together with the channel.

Not replaced by `select` with `ctx.Done()`: that would add a branch to every reply, and the thing it guards against is prevented by a single buffer slot.

The whole `mail` package is estimated at around 100 lines. For comparison: Orleans' `Scheduler/` measures 823 lines, of which `WorkItemGroup` is 336 — because .NET needs a custom `TaskScheduler` to guarantee returning to the same logical execution context after `await`. In Go, a goroutine is naturally an execution context; the problem does not exist.

## Channel capacity

Buffered or unbuffered directly changes the backpressure semantics:

- **Unbuffered**: the caller blocks until the entity starts processing. Backpressure propagates automatically, but one slow entity hangs all its callers.
- **Buffered**: absorbs bursts, but behavior at a full buffer must be defined — block or reject.

The choice: bounded buffer, reject when full (returning a clear overload error). An unbounded queue disguises a memory problem as a latency problem, and blocking lets one hot entity drag down the whole process. Capacity is configurable.

## Request order

Consecutive calls from the same caller to the same entity execute in the order they were initiated — the channel is FIFO, which holds naturally in the local case.

**No order guarantee across nodes.** Network reordering plus reconnection breaks it, and the complexity of sequence numbers and reorder buffers is not worth it. The docs must state this explicitly; users must not be led to assume an order guarantee.

## Relationship with synctest

That this design is fully observable by `testing/synctest` is not a coincidence but the reason for the choice:

- A goroutine blocked on a channel counts as "durably blocked"; `synctest.Wait()` can determine that the system is quiescent.
- If serialization were done with mutexes plus condition variables, `synctest` could not determine quiescence (mutex blocking is not durably blocking), and the whole testing strategy collapses.

So `sync.Mutex` must not appear in the `mail` package for cross-call waiting. A short critical section that merely protects a map is fine; using it to "wait" is not.

## Relationship with scheduled tasks

Persisted scheduled tasks (`Schedule`) do not run on the mailbox's clock. They are "a table plus a poller": the poller finds due items and constructs an ordinary call delivered to the target entity's mailbox.

So to the entity, a scheduled task is indistinguishable from an ordinary method call and enjoys the same serialization guarantee.

**Explicitly not a repeat of Orleans Reminders v1** — an in-memory cache plus ring partitioning plus complex ownership transfer, which Orleans itself replaced with `Orleans.DurableJobs` (v2, measured at 5278 lines, still preview). Table plus polling is dumber but easier to verify, and accurate enough — persisted scheduled tasks should never promise millisecond precision.

Details in [timers.md](timers.md).
