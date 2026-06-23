// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// Boost.Capy version of the queue/channel adapter.
//
// boost::capy::task<T>'s promise has a CLOSED await_transform: its
// transform_awaitable hard-requires the IoAwaitable protocol - an awaitable
// whose await_suspend takes (std::coroutine_handle<>, io_env const*) - and
// static_assert-s "requires IoAwaitable" for anything else. So the coro_util
// queue/channel awaitables (a foreign awaiter with the ordinary
// await_suspend(handle) signature) cannot be co_awaited inside a capy::task
// directly. capy_wrap() bridges the gap by presenting the queue awaitable to
// Capy as an IoAwaitable - the one extension point task's await_transform
// accepts for an arbitrary operation. It is driven by a small detached coroutine
// whose own promise has no await_transform and therefore CAN co_await the queue
// awaitable.
//
// Usage, inside any boost::capy::task<T> coroutine:
//
//   while (auto data = co_await coro_util::capy_wrap(q.pull())) {
//     consume(data.value());
//   }
//
//   co_await coro_util::capy_wrap(q.push(value));
//
// Executor affinity: the IoAwaitable protocol hands await_suspend the
// coroutine's io_env, whose executor is the one the task is bound to. We capture
// that executor and, once the queue waiter is woken on whatever thread, post the
// coroutine's resumption back to it (executor_ref::post). So the coroutine
// resumes on its home executor, not the waker's thread. (Capy otherwise resumes
// inline on whichever thread completed the operation.) Because affinity is
// restored here, the queues are bound to the inline continuation policy - which
// is exactly what the alias headers in this folder do.
//
// Lifetime note for push(): q.push(args...) binds references to args, so (as
// with a direct co_await q.push(...)) those arguments must stay alive until the
// co_await completes. Pass lvalues that outlive the statement, or temporaries
// (whose lifetime is extended across the co_await), not dangling references.

#include <boost/capy/continuation.hpp>
#include <boost/capy/ex/executor_ref.hpp>
#include <boost/capy/ex/io_env.hpp>

#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace coro_util {
namespace capy_detail {

// Detached coroutine used to drive a coro_util queue awaitable. Unlike
// capy::task, this promise defines NO await_transform, so an arbitrary awaiter
// can be co_awaited inside it. It owns nothing and self-destroys on completion
// (suspend_never at both ends), so once its single co_await resumes and the
// continuation is posted, its frame is freed.
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

// Result slot for the value case; empty for the void case so queue_op stays a
// continuation + the wrapped awaitable when nothing is returned.
template <typename R> struct result_storage {
  std::optional<R> value;
};
template <> struct result_storage<void> {};

template <typename Awaitable> struct queue_op;

// Driver for awaitables whose co_await yields void (e.g. push()/push_bulk()).
template <typename Awaitable>
driver_task
drive_void(Awaitable Aw, queue_op<Awaitable>* Self, boost::capy::executor_ref Ex) {
  co_await std::move(Aw);
  Ex.post(Self->cont);
}

// Driver for awaitables whose co_await yields a value (e.g. pull()'s scope). The
// result is stored back into the queue_op (which lives stably in the suspended
// coroutine's frame until await_resume) before the continuation is posted; the
// post()/dequeue provides the happens-before so await_resume sees it.
template <typename Awaitable>
driver_task
drive_value(Awaitable Aw, queue_op<Awaitable>* Self, boost::capy::executor_ref Ex) {
  Self->value.emplace(co_await std::move(Aw));
  Ex.post(Self->cont);
}

// An IoAwaitable that adapts a coro_util queue awaitable. capy::task recognizes
// it through its transform_awaitable (which requires the IoAwaitable protocol)
// and, on suspension, hands await_suspend the coroutine's io_env.
template <typename Awaitable> struct queue_op : result_storage<result_t<Awaitable>> {
  using R = result_t<Awaitable>;

  Awaitable aw;
  boost::capy::continuation cont{};

  explicit queue_op(
    Awaitable Aw
  ) noexcept(std::is_nothrow_move_constructible_v<Awaitable>)
      : aw(std::move(Aw)) {}

  // Always suspend: the driver owns the co_await of the wrapped awaitable, so
  // the continuation must be captured before any result is produced.
  bool await_ready() const noexcept { return false; }

  std::coroutine_handle<>
  await_suspend(std::coroutine_handle<> Handle, boost::capy::io_env const* Env) noexcept {
    cont.h = Handle;
    boost::capy::executor_ref Ex = Env->executor;
    if constexpr (std::is_void_v<R>) {
      drive_void(std::move(aw), this, Ex);
    } else {
      drive_value(std::move(aw), this, Ex);
    }
    return std::noop_coroutine();
  }

  R await_resume() noexcept {
    if constexpr (!std::is_void_v<R>) {
      return std::move(*this->value);
    }
  }
};

} // namespace capy_detail

/// Adapts any coro_util queue/channel awaitable (q.pull(), q.push(x),
/// q.push_bulk(...)) so it can be co_awaited inside a boost::capy::task<T>
/// coroutine. Returns an IoAwaitable that yields the same value the wrapped
/// awaitable would. See the file header for usage and affinity notes.
template <typename Awaitable> capy_detail::queue_op<Awaitable> capy_wrap(Awaitable Aw) {
  return capy_detail::queue_op<Awaitable>{std::move(Aw)};
}

} // namespace coro_util
