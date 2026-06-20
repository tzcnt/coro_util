// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// Both boost::asio::awaitable<T> and boost::asio::experimental::coro<> have a
// CLOSED await_transform: each only accepts other asio coroutines of its own
// kind, registered async operations, and this_coro::* tags, so the coro_util
// queue/channel awaitables (a foreign awaiter) cannot be co_awaited inside them
// directly. asio_queue_op() bridges the gap by presenting the queue awaitable to
// asio as a regular (deferred) async operation, which BOTH coroutine types adopt
// through their shared is_async_operation door. It is driven by a small detached
// coroutine whose own promise has no await_transform and therefore CAN co_await
// the queue awaitable.
//
// Usage, inside any boost::asio::awaitable<T> OR experimental::coro<> coroutine:
//
//   while (auto data = co_await coro_util::asio_queue_op(q.pull())) {
//     consume(data.value());
//   }
//
//   co_await coro_util::asio_queue_op(q.push(value));
//
// Executor affinity: the completion is always posted back to the suspended
// coroutine's associated executor (the io_context/strand it was spawned on), so
// the coroutine resumes on its own executor no matter which thread woke the queue
// waiter. (Both asio coroutine types resume inline on the completion handler's
// thread otherwise.) Because affinity is restored here, the queues should be
// bound to the inline continuation policy - which is exactly what the alias
// headers in this folder do.
//
// Lifetime note for push(): q.push(args...) binds references to args, so (as
// with a direct co_await q.push(...)) those arguments must stay alive until the
// co_await completes. Pass lvalues that outlive the statement, or temporaries
// (whose lifetime is extended across the co_await), not dangling references.

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/post.hpp>

#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

namespace coro_util {
namespace asio_detail {

// Detached coroutine used to drive a coro_util queue awaitable. Unlike
// boost::asio::awaitable<T>, this promise defines NO await_transform, so an
// arbitrary awaiter can be co_awaited inside it. It owns nothing and
// self-destroys on completion (suspend_never at both ends), so once its single
// co_await resumes and the completion handler is posted, its frame is freed.
struct driver_task {
  struct promise_type {
    driver_task get_return_object() noexcept { return {}; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };
};

// Obtain the awaiter for an awaitable, honoring operator co_await() when present
// (the coro_util queue awaitables expose `operator co_await() &&`).
template <typename Awaitable> decltype(auto) get_awaiter(Awaitable&& Aw) {
  if constexpr (requires { static_cast<Awaitable&&>(Aw).operator co_await(); })
    return static_cast<Awaitable&&>(Aw).operator co_await();
  else
    return static_cast<Awaitable&&>(Aw);
}

template <typename Awaitable>
using awaiter_t =
  std::remove_reference_t<decltype(get_awaiter(std::declval<Awaitable&&>()))>;

template <typename Awaitable>
using result_t = decltype(std::declval<awaiter_t<Awaitable>&>().await_resume());

// Driver for awaitables whose co_await yields void (e.g. push()/push_bulk()).
template <typename Awaitable, typename Handler>
driver_task drive_void(Awaitable Aw, Handler H) {
  auto Ex = boost::asio::get_associated_executor(H);
  co_await std::move(Aw);
  boost::asio::post(Ex, [H = std::move(H)]() mutable { std::move(H)(); });
}

// Driver for awaitables whose co_await yields a value (e.g. pull()'s scope).
template <typename Awaitable, typename Handler>
driver_task drive_value(Awaitable Aw, Handler H) {
  auto Ex = boost::asio::get_associated_executor(H);
  auto Result = co_await std::move(Aw);
  boost::asio::post(Ex, [H = std::move(H), Result = std::move(Result)]() mutable {
    std::move(H)(std::move(Result));
  });
}

} // namespace asio_detail

/// Adapts any coro_util queue/channel awaitable (q.pull(), q.push(x),
/// q.push_bulk(...)) so it can be co_awaited inside a boost::asio::awaitable<T>
/// or boost::asio::experimental::coro<> coroutine. Returns a deferred async
/// operation that yields the same value the wrapped awaitable would; the
/// enclosing coroutine binds its own completion token when it co_awaits it. See
/// the file header for usage and affinity notes.
template <typename Awaitable> auto asio_queue_op(Awaitable Aw) {
  using R = asio_detail::result_t<Awaitable>;
  if constexpr (std::is_void_v<R>) {
    return boost::asio::async_initiate<void()>(
      [](auto Handler, Awaitable Aw) {
        asio_detail::drive_void(std::move(Aw), std::move(Handler));
      },
      boost::asio::deferred, std::move(Aw)
    );
  } else {
    return boost::asio::async_initiate<void(R)>(
      [](auto Handler, Awaitable Aw) {
        asio_detail::drive_value(std::move(Aw), std::move(Handler));
      },
      boost::asio::deferred, std::move(Aw)
    );
  }
}

} // namespace coro_util
