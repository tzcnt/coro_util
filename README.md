# coro_util

## Supported Libraries

| Library | Adapter | Executor Affinity | Priority Affinity |
|---|---|---|---|
| TooManyCooks | [link](include/coro_util/adapter/tmc) | ✅ | ✅ |
| YACLib | [link](include/coro_util/adapter/yaclib) | ✅ | ❌ |
| Boost.Cobalt | [link](include/coro_util/adapter/cobalt) | ✅ | ❌ |
| concurrencpp | [link](include/coro_util/adapter/concurrencpp) | ❌ | ❌ |
| cppcoro | [link](include/coro_util/adapter/cppcoro) | ❌ | ❌ |
| libcoro | [link](include/coro_util/adapter/libcoro) | ❌ | ❌ |

## Adapting for Another Library
Want to use these queues with another library that doesn't have an adapter yet? Simply follow these instructions:
1. Check out this repo, and the other library, on your local machine.
2. From this repo, issue the following prompt to your agent:
```
@ADAPTER_PORTING_GUIDE.md add a new adapter for <other library>, which is checked out locally at <other library full path>
```

Feel free to open a PR into this repo with the new adapters. Do not commit the library-specific tests.

## Unsupported Libraries

- [libfork](https://github.com/ConorWilliams/libfork) cannot be adapted. Its
`lf::task` promise defines a closed set of `await_transform` overloads (fork,
call, join, etc.), with no generic passthrough. The `coro_util` queues suspend via a standard awaitable
(`bool await_suspend(std::coroutine_handle<Promise>)`), which matches none of
those overloads, so it won't compile inside an `lf::task`. A continuation policy
can't fix this: it only controls how a waiter is captured and resumed, not the
awaitable's signature, and the incompatibility is upstream in `await_transform`.
