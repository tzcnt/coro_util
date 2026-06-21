# coro_util

Header-only async data structures for C++20 coroutines.

These data structures have been white-labeled from the [TooManyCooks](https://github.com/tzcnt/TooManyCooks) framework and made adaptable for use with any C++20 coroutine library.

This repo does not provide a task or executor type. Instead, you configure the data structures with a small policy adapter that allows them to integrate with your library of choice. Pre-built adapters are provided for several well-known coroutine libraries.

## Queues

All queues are linearizable, lock-free, and wait-free on the fast path (unbounded / not-full bounded). Instead of spinning or blocking, participants suspend when data is not available.

All queues are zero-copy: elements are emplaced directly in the queue storage and are accessed on the consumer side via a scoped reference. You can choose to copy the data out of this reference, or use it in-place. This means that you can use these queues with types that have no default, copy, or move constructor.

All queues can be `close()` d, which immediately stops new producers, and also signals to consumers when the queue has been fully drained.

| Name | Purpose |
|---|---|
| `qu_spsc_bounded` | SPSC bounded queue |
| `qu_spsc_unbounded` | SPSC unbounded queue |
| `qu_mpsc_bounded` | MPSC bounded queue |
| `qu_mpsc_unbounded` | MPSC unbounded queue |
| `channel` | MPMC queue. accessed via `chan_tok` hazard pointer + shared ownership handle |

## Usage

Add `include/` to your include path, then include the queue header from the
adapter folder for your coroutine library. Each adapter binds the queues to that
library's continuation policy, implementing executor affinity for those libraries that support it.

```cpp
// Pick the adapter folder that matches your library (here: TooManyCooks).
#include "coro_util/adapter/tmc/qu_spsc_bounded.hpp"

tmc::task<void> example() {
  // A single-producer/single-consumer bounded queue holding 16 size_t slots.
  coro_util::qu_spsc_bounded<size_t> queue{16};

  // Producer side. push() for bounded queues suspends until a slot is free.
  // post() for unbounded queues does not suspend.
  co_await queue.push(42);

  // Consumer side. pull() suspends until an element is available and returns a
  // zero-copy handle to the slot. The handle is empty (operator bool == false)
  // only once the queue is closed and drained;.
  while(auto value = co_await queue.pull()) {
    process(*value);
  }
}
```

Swapping libraries is just swapping the include directory: use
`coro_util/adapter/yaclib/...`, `coro_util/adapter/asio/...`, etc.

## Provided Library Adapters

| Library | Adapter | Executor Affinity | Priority Affinity |
|---|---|---|---|
| [TooManyCooks](https://github.com/tzcnt/TooManyCooks) | [link](include/coro_util/adapter/tmc) | ✅ | ✅ |
| [YACLib](https://github.com/YACLib/YACLib) | [link](include/coro_util/adapter/yaclib) | ✅ | ❌ |
| [Boost.Cobalt](https://github.com/boostorg/cobalt) | [link](include/coro_util/adapter/cobalt) | ✅ | ❌ |
| [Asio / Boost.Asio](https://github.com/chriskohlhoff/asio) | [link](include/coro_util/adapter/asio) | ✅¹ ² | ❌ |
| [libfork](https://github.com/ConorWilliams/libfork) | [link](include/coro_util/adapter/libfork) | ✅² | ❌ |
| [concurrencpp](https://github.com/David-Haim/concurrencpp) | [link](include/coro_util/adapter/concurrencpp) | ❌ | ❌ |
| [cppcoro](https://github.com/andreasbuhr/cppcoro) | [link](include/coro_util/adapter/cppcoro) | ❌ | ❌ |
| [libcoro](https://github.com/jbaldwin/libcoro) | [link](include/coro_util/adapter/libcoro) | ❌ | ❌ |

¹ Works with both standalone Asio (include `adapter/asio/op.hpp`, `asio::`) and
Boost.Asio (include `adapter/asio/boost_op.hpp`, `boost::asio::`), and covers
both the `awaitable<T>` and `experimental::coro<>` coroutine types of each.

² These have a closed `await_transform` and cannot `co_await` a foreign
awaitable directly, so each queue/channel operation must be wrapped at the call
site: `co_await coro_util::asio_queue_op(q.pull())` for Asio,
`co_await coro_util::lf_queue_op(q.pull())` for libfork.

## Adapting for Another Library
Want to use these queues with another library that doesn't have an adapter yet? Simply follow these instructions:
1. Check out this repo, and the other library, on your local machine.
2. From this repo, issue the following prompt to your agent:
```
@ADAPTER_PORTING_GUIDE.md add a new adapter for <other library>, which is checked out locally at <other library full path>
```

Feel free to open a PR into this repo with the new adapters. Do not commit the library-specific tests.
