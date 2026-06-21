# coro_util

## Supported Libraries

| Library | Adapter | Executor Affinity | Priority Affinity |
|---|---|---|---|
| TooManyCooks | [link](include/coro_util/adapter/tmc) | ✅ | ✅ |
| YACLib | [link](include/coro_util/adapter/yaclib) | ✅ | ❌ |
| Boost.Cobalt | [link](include/coro_util/adapter/cobalt) | ✅ | ❌ |
| Asio / Boost.Asio | [link](include/coro_util/adapter/asio) | ✅¹ ² | ❌ |
| libfork | [link](include/coro_util/adapter/libfork) | ✅² | ❌ |
| concurrencpp | [link](include/coro_util/adapter/concurrencpp) | ❌ | ❌ |
| cppcoro | [link](include/coro_util/adapter/cppcoro) | ❌ | ❌ |
| libcoro | [link](include/coro_util/adapter/libcoro) | ❌ | ❌ |

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
