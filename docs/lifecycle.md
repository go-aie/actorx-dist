# Lifecycle

English | [中文](lifecycle.zh.md)

Request completion and object exit are separate boundaries. [Call semantics](runtime.md) defines Send/Call/Stream completion; complete instance exit also joins handlers, tasks and cleanup before committing distributed terminal evidence. Business Started/Stopping/Stopped rules follow the [actorx lifecycle](https://github.com/go-aie/actorx/blob/main/docs/lifecycle.md).

## Object lifetime

| Scenario | Cleanup responsibility |
| --- | --- |
| Shared object | The application defines when to stop; one request or caller closing does not end shared use |
| Exclusive temporary object | Retain the original Addr, Stop → Wait with an independent cleanup deadline, then close the caller Runtime |
| Collaborating Actors | Addresses and calls do not propagate stop; the application ends exclusive dependencies explicitly |
| Type withdrawal | Registration.Unregister closes that registration's creation capability and stops/joins its logical instances; see [registration](runtime.md#registration-and-withdrawal) |
| Owning provider exit | Runtime.Close joins all owned generations, including retained failed activations and local children |

Dropping Addr, ending a call, closing its caller or stopping its initiating Actor leaves remote objects alive. Communicating after omitting ID addresses a new object; there is no idle collection. Self provides neither automatic unsubscribe nor cascading stop.

## Stop and Wait

| Operation | Success establishes |
| --- | --- |
| `addr.Stop(cause)` | Admission closed for this generation, without joining handlers, tasks or Stopped. The first accepted cause survives repeated Stop |
| `addr.Wait(ctx)` | Generation cleanup and required terminal submission completed, returning actorx.Exit. Exit.Reason is the final cause; the separate error reports waiting/observation failure |

Stop uses a two-second control deadline. Wait defaults to five seconds without a deadline and caps explicit deadlines at sixty seconds. Wait can be repeated after timeout without canceling exit or submission.

```go
// Exclusive temporary object: original address, independent deadline, before caller Close.
cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := addr.Stop(nil); err != nil {
    return err
}

exit, err := addr.Wait(cleanup)
// Check err; when err == nil, exit.Reason is the final cause.
```

Cold Stop/Wait only looks up the current generation without creating one; absence returns ErrNotActivated. Existing addresses, including copies and message round trips, always control the original generation. After ordinary Wait a fresh address can activate another generation; old Stop cannot affect it. Old-generation communication returns ErrStopped. Missing/expired terminal evidence returns ErrExitUnknown; unreachable owners may yield ErrOwnerUnavailable/ErrActivationUnknown.

Retain the original address for reconciliation and cleanup even if the first business call fails. Failed Stop/Wait does not establish completed cleanup. Shared address state caches only that generation's committed Exit, not its instance. Cached results remain readable after the observer Runtime.Close; uncached Wait returns ErrClosed.

## Failed activations

Nil/panicking New, mismatched bindings and failed Spawn/Started must finish rollback before committing terminal failure. **Wait can successfully observe failure while the key and generation budget remain held**, without automatic reconstruction. Only Stop or owner Runtime.Close releases them. Failed-activation Stop waits for committed release without changing the cause; cached failure Exit does not establish release either.

Ordinary business errors end requests; panic, self-stop and explicit Stop end instances. See [Registry records and operations](registry.md#records-and-conditional-operations) for complete state and submission evidence.

## Runtime.Close

Close synchronously stops this Runtime's new call/creation admission. Background work stops and joins owned instances, local descendants, in-flight work and terminal submission before closing management and network resources. Pure caller Close leaves remote objects alive.

Close timeout ends only this wait; background cleanup continues and another Close can wait again. Blocked application loop bodies keep their budget. Unwritable KV retains evidence, work and connection needed for submission until storage repair. Neither local cleanup nor one timeout establishes complete shutdown.

See [architecture](architecture.md#shutdown-and-fault-handoffs) for internal shutdown order. After a crash, fence the original owner before [explicit recovery](deployment.md#explicit-recovery-and-storage-repair); missing evidence cannot become a fabricated Exit.
