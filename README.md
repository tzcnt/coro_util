# coro_util

## Unsupported Libraries

- [libfork](https://github.com/ConorWilliams/libfork) cannot be adapted. Its
`lf::task` promise defines a closed set of `await_transform` overloads (fork,
call, join, etc.), with no generic passthrough. The `coro_util` queues suspend via a standard awaitable
(`bool await_suspend(std::coroutine_handle<Promise>)`), which matches none of
those overloads, so it won't compile inside an `lf::task`. A continuation policy
can't fix this: it only controls how a waiter is captured and resumed, not the
awaitable's signature, and the incompatibility is upstream in `await_transform`.
