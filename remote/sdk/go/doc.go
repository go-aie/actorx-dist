// Package actorxremote connects Go providers and consumers to an
// actorx.remote/v1 gateway.
//
// NewRuntime and Runtime.Addr validate locally. Addr.Call sends JSON using only
// HTTP, without actorx, NATS, redirects, environment proxies or automatic retries.
// Inputs follow encoding/json rules; outputs use json.Number to preserve numbers.
// Each Addr serializes its operations and retains the first selected generation.
// A cold address with an uncertain outcome refuses further calls.
//
// Runtime.Register installs a provider session without constructing an actor.
// The first create calls ActorFactory; subsequent Calls and Streams on the same
// complete identity share that object and its execution lock. Each registration
// owns at most eight instances and eight execution slots, including queued work.
// Factory, handler and JSON errors/panics become controlled RPC failures.
// Register's context limits installation only; Runtime owns the live SSE.
// A provider connection failure closes the Runtime and WaitClosed reports why.
//
// Close cancels and joins runtime-owned work. A canceled Close context ends only
// that wait; cleanup continues and Close or WaitClosed can be called again.
// User handlers must cooperate with context cancellation and factories must
// return promptly: Go cannot forcibly stop them, and their locks and execution
// slots remain held until they actually return.
//
// Addr.Stream validates stream_open before returning an explicitly owned Stream.
// Iterate with Next/Value, then check Err. Next acknowledges the previous item;
// Close is required after an early break and may interrupt a blocked Next.
// Streams hold their Addr operation lock through consumption or Close.
// StreamActor writes JSON chunks through StreamWriter; the SDK supplies one
// terminal when the handler returns. Both segments independently account for
// four items and 32 KiB, including the complete encoded chunk envelope.
package actorxremote
