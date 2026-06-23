# Adapter Porting Guide

How to bind the runtime-agnostic `coro_util` queues/channel to a new coroutine
library by writing a **continuation policy** and a set of thin alias headers,
then proving it works (especially executor affinity).

This guide generalizes the process used to build the TMC and YACLib adapters
(`include/coro_util/adapter/tmc`, `include/coro_util/adapter/yaclib`). Read the
YACLib adapter alongside this document — it is the smaller, cleaner reference.

There are two routes. The **policy route** (§§1–6) is the default and covers most
libraries. Some libraries' coroutines refuse a foreign awaitable outright — their
promise has a *closed* `await_transform` — and then no policy will even compile;
those take the **call-site wrapper route** (§7), for which Asio
(`include/coro_util/adapter/asio`) and libfork
(`include/coro_util/adapter/libfork`) are the references.

---

## 0. Background: what an adapter is

The queues in `include/coro_util/*.hpp` are runtime-agnostic. They never name a
specific executor type. Instead each is a template:

```cpp
template <typename ContinuationPolicy, typename T, typename Config>
class qu_spsc_bounded_impl { ... };
```

When a consumer/producer coroutine has to wait (queue empty / full), the queue:

1. **captures** the awaiting coroutine's resume context, by calling
   `ContinuationPolicy::capture(handle)` and storing the returned `state`
   next to the type-erased `std::coroutine_handle<>` continuation; then
2. later **resumes** it by calling `ContinuationPolicy::resume(state, handle)`
   (or `resume_inline`).

An *adapter* supplies that policy for one specific library, plus a folder of
`using`-alias headers so a user of that library writes `coro_util::qu_spsc_bounded<T>`
and gets the queue pre-bound to the right policy.

There is also a generic, ready-to-use policy at
`include/coro_util/adapter/inline/policy.hpp` (resumes everything inline, no
affinity) — both the minimal example of the contract and a real policy you can
bind to directly.

> **If the target library has no executor affinity to capture, do not write a
> new policy at all — reuse `inline/policy.hpp`.** A library whose coroutines
> carry no executor/priority/strand that you could read at `capture()` time (and
> exposes no "current executor" you could query) has nothing for a bespoke
> policy to do beyond `Continuation.resume()`, which is exactly what the inline
> policy already provides. In that case skip sections 3 and the standalone
> compile check; your alias headers just `#include "../inline/policy.hpp"` and
> bind `coro_util::detail::inline_continuation_policy`. The concurrencpp adapter
> (`include/coro_util/adapter/concurrencpp`) is the reference for this path:
> concurrencpp stores no capturable affinity and has no public
> `get_current_executor()`, so every one of its alias headers binds the generic
> inline policy. Once such a library *does* gain a way to observe the current
> executor, revisit and write a real policy per sections 2–3.

> **If the target library's coroutine has a *closed* `await_transform`, no policy
> will compile at all — take the call-site wrapper route instead (see §7).** Some
> coroutine types (Asio's `awaitable<T>` / `experimental::coro<>`, libfork's
> `lf::task`) accept only a fixed allow-list of awaitables via `await_transform`,
> with no generic passthrough, so `co_await q.pull()` is rejected inside them no
> matter which policy the queue is bound to. The fix is not a policy but a wrapper
> that re-presents each queue op through the library's own extension point. The
> policy machinery (§§1–3) does not apply; §7 is a self-contained alternative to
> §§1–6.

---

## 1. The policy contract

A continuation policy is a struct with a nested `state` type and three static
functions. This is the **entire** interface the base queues depend on:

```cpp
struct my_continuation_policy {
  // Per-waiter captured context. MUST be default-constructible
  // (the queues static_assert this). Keep it small & trivially copyable.
  struct state { /* ...captured fields... */ };

  // Called from the awaitable's await_suspend, with the TYPED handle of the
  // suspending coroutine. This is your only chance to read the coroutine's
  // promise. Capture whatever resume() will need.
  template <typename Promise>
  static state capture(std::coroutine_handle<Promise> Handle) noexcept;

  // Resume the waiter "properly" — i.e. honoring whatever affinity the library
  // has (executor, priority, strand, ...). Called from another coroutine that
  // just made progress (a producer waking a consumer, etc.). The handle is
  // type-erased; if you need the promise, you must have captured it in state.
  static void resume(state& State, std::coroutine_handle<> Continuation) noexcept;

  // Resume immediately on the caller's stack, ignoring affinity. Used by the
  // queues' *_resume_inline paths (e.g. close_resume_inline()).
  static void resume_inline(state& State, std::coroutine_handle<> Continuation) noexcept;
};
```

Key facts that drive the whole design:

- **`capture` gets the typed handle; `resume` does not.** `await_suspend` in the
  base impl is `template <typename Promise> bool await_suspend(std::coroutine_handle<Promise> Outer)`.
  So `capture(Outer)` can reach `Outer.promise()`. By the time `resume` runs, the
  handle is `std::coroutine_handle<>` (type-erased). **Anything `resume` needs
  from the promise must be saved into `state` during `capture`.**
- **`state` must be default-constructible** and is stored inline in every queue
  waiter slot, so keep it to a couple of pointers/ints.
- `resume_inline` is almost always just `Continuation.resume()`.

The two existing policies bracket the design space:

| Policy | `state` | `resume` | affinity |
|---|---|---|---|
| `inline` | empty | `Continuation.resume()` | none |
| `tmc` | `{ex_any* executor; size_t priority;}` | `post_checked(executor, cont, priority)` | executor + priority, from thread-locals |
| `yaclib` | `{IExecutor* executor; Job* job;}` | `executor->Submit(*job)` | executor, from the promise |

---

## 2. Investigate the target library

The goal is to answer four questions. Do this by reading the library's headers
(and a couple of its own tests), not by guessing.

1. **What is the unit of affinity?** Executor? Executor + priority? Strand? A
   library with no scheduler concept — or one whose coroutines expose no
   capturable affinity and no queryable "current executor" (concurrencpp) →
   stop here and reuse the generic `inline/policy.hpp` (see the callout in
   §0); there is nothing for a bespoke policy to capture.
2. **Where does the library keep "where should this coroutine resume"?**
   Two common models:
   - **Thread-local** (TMC): `capture` reads a thread-local "current executor"
     (`tmc::current_executor()` / `current_priority()`).
   - **In the coroutine promise** (YACLib): `capture` reads it off
     `Handle.promise()` (YACLib stores `BaseCore::_executor`). This is why
     `capture` is handed the *typed* handle.
3. **How do you reschedule a bare/suspended coroutine onto that affinity?**
   Find the library's own "resume on executor" primitive and mirror it. In
   YACLib the coroutine promise *is* a `Job`, and `On()`/`Yield()` reschedule by
   `executor->Submit(promise)` — so the policy does exactly that, allocation-free.
   In TMC it's `tmc::detail::post_checked(executor, handle, priority)`.
4. **What is the lifetime / refcount contract of that reschedule call?** Confirm
   it neither leaks nor double-frees the coroutine. For YACLib this meant
   reading `FairThreadPool::Submit` + the worker loop to confirm `Submit` just
   enqueues and `Job::Call()` only resumes the coroutine (no refcount change,
   no delete) — identical to what `On()` relies on. **Do not skip this step**;
   it is the difference between "compiles" and "correct".

Practical tactics that worked:
- `grep` the library for its `On`/`resume_on`/`schedule`/`post` primitive and
  read how *it* captures and reschedules. Copy that exact mechanism.
- Trace the inheritance chain to confirm assumptions (e.g. that the promise is a
  `Job`: `PromiseType → … → BaseCore → InlineCore → Job`, all public).
- Read the library's own coroutine tests (e.g. `test/unit/coro/on.cpp`) to learn
  the *user-facing* API you'll need for the tests in step 5 (how to make a thread
  pool, run a coroutine on it, wait for it, identify the running thread).

---

## 3. Write the policy header

> Skip this entire section if step 2 concluded the library has no capturable
> affinity — bind `inline/policy.hpp` instead (see the §0 callout) and jump to §4.

`include/coro_util/adapter/<lib>/policy.hpp`. Use the YACLib one as the template.

```cpp
#pragma once
#include "<lib>/...executor.h..."   // the executor/scheduler type
#include "<lib>/...job/handle...h"  // whatever resume() submits
#include <coroutine>

namespace coro_util {
namespace detail {
struct <lib>_continuation_policy {
  struct state { /* pointers captured from promise or thread-locals */ };

  template <typename Promise>
  static state capture(std::coroutine_handle<Promise> Handle) noexcept {
    // read thread-locals, or Handle.promise().<affinity fields>
  }

  static void resume(state& State, std::coroutine_handle<> Continuation) noexcept {
    // mirror the library's own "resume on executor" primitive
  }

  static void resume_inline(state&, std::coroutine_handle<> Continuation) noexcept {
    Continuation.resume();
  }
};
} // namespace detail
} // namespace coro_util
```

Notes / pitfalls:
- Put the policy struct in `coro_util::detail`.
- The base impl `static_assert`s `state` is default-constructible.
- If `capture` needs to convert the promise to some base type (e.g. `Job*`),
  rely on the same implicit upcast the library itself uses, and verify it's an
  accessible, unambiguous base.
- **TMC-only wrinkle:** the TMC policy also specializes
  `tmc::detail::awaitable_traits` so the awaitables are recognized as "known"
  TMC awaitables. That is a TMC-specific optimization — **most libraries do not
  need anything like it.** Don't copy it unless the target library has an
  analogous awaitable-registration mechanism.

---

## 4. Write the alias headers

One per queue + the channel. These are mechanical — copy the YACLib ones and
swap the policy name. (For an affinity-less library, copy the concurrencpp ones
instead: they `#include "../inline/policy.hpp"` and bind
`coro_util::detail::inline_continuation_policy`.) For an affinity-less library,
also follow the concurrencpp comment structure: a single leading `// Provides
coro_util::<alias>, ...` line that ends with the short parenthetical
`(<lib> does not support executor affinity)` — don't elaborate on *why* the
library lacks affinity in the header (that belongs in the investigation notes,
not five copies of a comment). There are five:

```
include/coro_util/adapter/<lib>/qu_spsc_bounded.hpp
include/coro_util/adapter/<lib>/qu_spsc_unbounded.hpp
include/coro_util/adapter/<lib>/qu_mpsc_bounded.hpp
include/coro_util/adapter/<lib>/qu_mpsc_unbounded.hpp
include/coro_util/adapter/<lib>/channel.hpp
```

Each looks like:

```cpp
#pragma once
#include "policy.hpp"
#include "../../qu_spsc_bounded.hpp"   // the base impl (relative include)

namespace coro_util {
template <typename T, typename Config = coro_util::qu_spsc_bounded_default_config>
using qu_spsc_bounded =
  coro_util::qu_spsc_bounded_impl<coro_util::detail::<lib>_continuation_policy, T, Config>;
}
```

`channel.hpp` additionally re-exposes the factory:

```cpp
template <typename T, typename Config = coro_util::chan_default_config>
using chan_tok = coro_util::chan_tok_impl<coro_util::detail::<lib>_continuation_policy, T, Config>;

template <typename T, typename Config = coro_util::chan_default_config>
inline chan_tok<T, Config> make_channel() noexcept {
  return coro_util::detail::make_channel<coro_util::detail::<lib>_continuation_policy, T, Config>();
}
```

Sanity-check the policy compiles against the real library headers before moving
on. A fast standalone check (no full project build) — drive `capture`/`resume`
through a real coroutine of the target library so the templates instantiate:

```cpp
#include "coro_util/adapter/<lib>/policy.hpp"
#include "<lib>/.../task_or_future.h"
struct probe {
  bool await_ready() const noexcept { return false; }
  template <typename P> bool await_suspend(std::coroutine_handle<P> h) noexcept {
    auto s = coro_util::detail::<lib>_continuation_policy::capture(h);
    std::coroutine_handle<> e = h;
    coro_util::detail::<lib>_continuation_policy::resume(s, e);
    coro_util::detail::<lib>_continuation_policy::resume_inline(s, e);
    return false;
  }
  void await_resume() const noexcept {}
};
<lib>::Task<> coro() { co_await probe{}; }   // forces instantiation
```
Compile with `-fsyntax-only -I include -I <lib>/include`. If the library has a
generated config header (YACLib's `config.hpp.in`), you'll need to generate it
or point at a build that has. This catches member-access / conversion mistakes
in seconds.

---

## 5. Write self-contained tests

Create a self-contained `tests_<lib>/` folder with its **own** `CMakeLists.txt`,
kept out of the top-level `CMakeLists.txt` so it doesn't pull a new dependency
into the main build. These tests are disposable scaffolding — but **do not
delete them automatically**; leave them on disk after the adapter is confirmed
so the user can review them and decide when to remove them. They should not be
committed (see the README note), but that is the user's call to make, not yours.
Structure: `CMakeLists.txt`, `main.cpp` (gtest entry), `test_<lib>.cpp`.

CMake essentials:
- `FetchContent` the target library (point `SOURCE_DIR` at the local checkout so
  it's offline/deterministic) and GoogleTest.
- Set any flags the library needs **before** `FetchContent_MakeAvailable`
  (YACLib: `set(YACLIB_FLAGS CORO)` + `set(YACLIB_CXX_STANDARD 20)` to compile
  its coroutine sources).
- `target_include_directories(... ../include/coro_util)` so the
  `adapter/<lib>/...` include style resolves.
- `gtest_discover_tests`.

Don't test the queue internals exhaustively — that's covered by the main TMC
suite, and the queue logic is policy-independent. Test only:

1. **A couple of API round-trips** (one queue + the channel): producer/consumer
   coroutines on a multi-thread pool, push/post N items, assert the sum. Mostly
   proves the aliases compile and the resume path works under real scheduling.
2. **Executor affinity — the one thing that actually changed.** This is the test
   that matters. Pattern:
   - Two single-thread pools `A` and `B`; record each pool's worker thread id
     (run a trivial task on each and read `this_thread::get_id()`).
   - Consumer coroutine: `On(A)`, set a "ready" atomic, then `co_await pull()` on
     an **empty** queue → it suspends, capturing `A`.
   - Producer coroutine: `On(B)`, spin until "ready", sleep ~100ms (so the
     consumer is genuinely parked), then push → this wakes the consumer.
   - Assert the consumer resumed on **A's** thread, not B's. With affinity it
     runs on A; an inline policy would run it on B (the waker).

API gotchas learned the hard way:
- The **channel and the unbounded MPSC queue** use non-suspending `post()` for
  the producer side, **not** `co_await push()`. Only the bounded queues have a
  suspending `push()`. Check the actual header before writing producers.
- Zero-copy pull scopes must be released before the next call on the same
  token/queue; the `while (auto v = co_await q.pull()) { ... }` idiom handles it.

---

## 6. Validate — and falsify

Build and run. All green is necessary but **not sufficient**: an affinity test
that always passes proves nothing. Confirm it actually discriminates:

1. Temporarily change the policy's `resume` to `Continuation.resume()` (inline).
2. Rebuild, run the affinity test → it **must fail** (consumer resumes on the
   waker's thread).
3. Revert. Run the full suite → all green.

This falsification step is what turns "the test passed" into "the test verifies
executor affinity." It was the final check for the YACLib adapter and caught
nothing only because the design was already right — but run it every time.

Once everything is green, **stop and leave the `tests_<lib>/` folder in place.**
Do not delete it as a cleanup step — the user reviews the tests themselves and
decides whether to keep or remove them. Report that the tests are passing and
where they live; let the user drive the teardown.

---

## 7. Fallback: the call-site wrapper (closed `await_transform`)

Take this route only when the policy route can't compile: the target library's
coroutine promise has a **closed `await_transform`** — a fixed allow-list of
accepted awaitable types (the library's own tasks, registered operations, a few
tag types) with no generic passthrough. Inside such a coroutine `co_await
q.pull()` is rejected ("no matching `await_transform`"), and **no continuation
policy can fix it**: a policy governs how a waiter is captured and resumed, not
what the host coroutine's `await_transform` accepts. References:
`include/coro_util/adapter/asio/op.hpp` and
`include/coro_util/adapter/libfork/op.hpp`.

**Detect it** with the §4 standalone probe, but write the probe coroutine as the
*target library's own* coroutine type and bind the inline policy. If `co_await
probe{}` fails with an `await_transform` error, you are on this route. (If a plain
foreign awaitable compiles inside the library's coroutine, you are not — write a
normal policy.)

### The idea: two pieces

1. **A driver coroutine you own.** Define a tiny detached coroutine type whose
   promise has **no `await_transform`** — so it *can* `co_await` the foreign queue
   awaitable that the host coroutine rejects. It does nothing but run the real
   `co_await q.pull()` / `q.push(x)` and hand the result back. Make it own nothing
   and self-destroy: `suspend_never` at both ends, `terminate()` on exception.
2. **A door into the host coroutine.** Find the one extension point the host's
   closed `await_transform` *does* accept, and present the operation through it.
   The host's suspend hook then hands you a handle/token you use to resume the
   host once the driver completes.

`<lib>_wrap(awaitable)` is the wrapper function the user calls; it returns
whatever the door expects. The queues stay bound to the **inline policy** (alias
headers identical to an affinity-less library, §4) — affinity is restored by the
wrapper, not by a policy.

### Finding the door (library-specific)

This is the creative part: look for any mechanism by which the host coroutine
already accepts "something to wait on" that isn't one of its own tasks.

- **Asio** — `is_async_operation`. Present the queue op as a `deferred` async
  operation; both `awaitable<T>` and `experimental::coro<>` adopt async ops
  through that shared door. The driver runs the inner `co_await`, then invokes the
  completion handler asio gave it.
- **libfork** — the `context_switcher` concept: an awaitable natively exposing
  `await_ready()->bool`, `await_suspend(submit_handle)->void`, `await_resume()`.
  libfork's `await_transform` accepts any `context_switcher` and hands its
  `await_suspend` the host coroutine's `submit_handle`.

If the library exposes **no** such door — no async-op concept, no
switcher/"resume on" extension that accepts a foreign operation — then even a
wrapper is impossible; say so and stop. Search hard first: libfork was initially
judged un-adaptable until `context_switcher` turned out to be the door.

### Restoring executor affinity (the wrapper's job)

Because the queues use the inline policy, the queue resumes the **driver** inline
on whatever thread woke the waiter (the producer's thread). The wrapper must
itself send the *host* coroutine back to its own executor:

1. In the door's suspend hook (which runs on the host's thread), **capture the
   host's executor** — from a thread-local "current executor", or from the
   handle/token the door gave you.
2. Launch the driver to `co_await` the queue awaitable.
3. When the driver's inner `co_await` completes, **re-dispatch the host's resume
   onto the captured executor** (`post`/`schedule`/`submit`), carrying the result
   for value-yielding ops. The host now resumes on its home executor regardless of
   which thread woke the queue.

Asio does step 3 with `asio::post(get_associated_executor(handler), ...)`;
libfork with `home_ctx->schedule(submit_handle)`. This is the wrapper's analogue
of a policy's `resume()`.

### Result plumbing, lifetime, ordering

- **void vs value.** `push()`/`push_bulk()` yield void; `pull()` yields a
  zero-copy scope. Compute the result type generically
  (`decltype(awaiter.await_resume())`) and branch on `void`. Store a value result
  where it survives until the host resumes — carried inside the re-dispatch
  (Asio's posted lambda) or in the wrapper object that lives in the host frame
  (libfork's `queue_op`).
- **Happens-before.** The re-dispatch (`post`/`schedule`) is the synchronization
  edge: write the result, *then* re-dispatch; the host reads it after it resumes.
  The driver must **not** touch the wrapper/host state after re-dispatching — the
  host may already be running on another thread.
- **Synchronous completion.** If the queue is ready, the driver completes inline
  *inside* the host's suspend hook, so the re-dispatch fires synchronously there.
  Confirm the library allows scheduling/resuming from inside its own suspend hook
  (libfork's `resume_on` does exactly this; Asio's `post` is always safe).

### API cost — document it

Unlike a policy adapter (transparent `co_await q.pull()`), the wrapper is
**visible in user code**: every suspending op must be wrapped —
`co_await coro_util::<lib>_wrap(q.pull())`,
`co_await coro_util::<lib>_wrap(q.push(x))`. Non-suspending `post()` (channel
/ unbounded MPSC producer) needs no wrapper. Note the `push()` lifetime caveat:
`q.push(args...)` binds references to `args`, which must outlive the co_await.

### Tests and falsification

Same as §5–§6, except the affinity restoration lives in the wrapper, not a
policy. Falsify by making the wrapper re-dispatch onto the **waker's** executor
(e.g. the thread-local "current executor" read at completion time) instead of the
captured home executor — the affinity test must then fail. Both reference adapters
ship a header-only `tests_<lib>/` that does exactly this.

---

## 8. Checklist for a new `<lib>` adapter

Policy route (§§1–6):

- [ ] Identified the affinity unit and where the library stores it (thread-local
      vs promise) — **or confirmed there is none**, in which case reuse
      `inline/policy.hpp` and skip the next four items.
- [ ] Found and read the library's own "resume on executor" primitive.
- [ ] Confirmed the reschedule call's refcount/lifetime contract (no leak/double-free).
- [ ] `policy.hpp` written; `state` default-constructible & small.
- [ ] Standalone `-fsyntax-only` instantiation check passes against real headers.
- [ ] Five alias headers written (4 queues + channel), binding the chosen policy.
- [ ] `tests_<lib>/` with its own CMake, not wired into the top-level build.
- [ ] Round-trip tests + an affinity test, all passing.
- [ ] Falsification check: inline `resume` makes the affinity test fail; reverted.
- [ ] Decided whether any library-specific awaitable registration (cf. TMC's
      `awaitable_traits`) is needed — usually not.
- [ ] Left `tests_<lib>/` in place for the user to review — **not** auto-deleted.

Wrapper route instead (§7), when the library has a closed `await_transform`:

- [ ] Confirmed via the standalone probe that the library's own coroutine rejects
      a foreign awaitable (`await_transform` error) — so the policy route can't compile.
- [ ] Found the host coroutine's extension-point "door" (async-op concept,
      context-switcher, etc.) — **or confirmed none exists**, making it un-adaptable.
- [ ] Driver coroutine written (no `await_transform`; `suspend_never` both ends).
- [ ] `<lib>_wrap()` wrapper presents the op through the door; void and value
      result paths both handled.
- [ ] Affinity restored in the wrapper: host executor captured at suspend, host
      re-dispatched onto it (`post`/`schedule`) on completion.
- [ ] Alias headers bind the **inline** policy (affinity is in the wrapper).
- [ ] Round-trip + affinity tests pass; falsification (re-dispatch onto the
      waker's executor) makes the affinity test fail; reverted.
- [ ] Documented that users must wrap every suspending op at the call site.
```
