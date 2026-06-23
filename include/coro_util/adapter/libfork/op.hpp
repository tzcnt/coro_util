// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// libfork version of the queue/channel adapter.
//
// lf::task's promise has a CLOSED set of await_transform overloads (co_new,
// context_switcher, join, fork/call packets, just), so the coro_util
// queue/channel awaitables (a foreign awaiter) cannot be co_awaited inside an
// lf::task directly. lf_wrap() bridges the gap by presenting the queue
// awaitable to libfork as an lf::core::context_switcher - the one open extension
// point libfork's await_transform accepts for arbitrary awaitables. It is driven
// by a small detached coroutine whose own promise has no await_transform and
// therefore CAN co_await the queue awaitable.
//
// Usage, inside any lf::task<T> coroutine:
//
//   while (auto data = co_await coro_util::lf_wrap(q.pull())) {
//     consume(data.value());
//   }
//
//   co_await coro_util::lf_wrap(q.push(value));
//
// Executor affinity: when the coroutine suspends, libfork hands the context
// switcher an lf::submit_handle for it. We capture the worker_context the
// coroutine is currently running on (lf::impl::tls::context(), the same source
// lf::resume_on reads) and, once the queue waiter is woken on whatever thread,
// reschedule the handle onto that captured context via worker_context::schedule.
// So the coroutine resumes on its home worker, not the waker's thread. Because
// affinity is restored here, the queues are bound to the inline continuation
// policy - which is exactly what the alias headers in this folder do.
//
// Lifetime note for push(): q.push(args...) binds references to args, so (as
// with a direct co_await q.push(...)) those arguments must stay alive until the
// co_await completes. Pass lvalues that outlive the statement, or temporaries
// (whose lifetime is extended across the co_await), not dangling references.

#include <libfork/core/ext/context.hpp> // for worker_context
#include <libfork/core/ext/handles.hpp> // for submit_handle
#include <libfork/core/ext/tls.hpp>     // for impl::tls::context()

#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace coro_util {
namespace libfork_detail {

// Detached coroutine used to drive a coro_util queue awaitable. Unlike lf::task,
// this promise defines NO await_transform, so an arbitrary awaiter can be
// co_awaited inside it. It owns nothing and self-destroys on completion
// (suspend_never at both ends), so once its single co_await resumes and the
// libfork handle is rescheduled, its frame is freed.
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
// single pointer + the wrapped awaitable when nothing is returned.
template <typename R> struct result_storage {
  std::optional<R> value;
};
template <> struct result_storage<void> {};

template <typename Awaitable> struct queue_op;

// Driver for awaitables whose co_await yields void (e.g. push()/push_bulk()).
template <typename Awaitable>
driver_task drive_void(Awaitable Aw, lf::worker_context* Ctx, lf::submit_handle Handle) {
  co_await std::move(Aw);
  Ctx->schedule(Handle);
}

// Driver for awaitables whose co_await yields a value (e.g. pull()'s scope). The
// result is stored back into the queue_op (which lives stably in the suspended
// coroutine's frame until await_resume) before the handle is rescheduled; the
// schedule()/notify provides the happens-before so await_resume sees it.
template <typename Awaitable>
driver_task drive_value(
  Awaitable Aw, queue_op<Awaitable>* Self, lf::worker_context* Ctx,
  lf::submit_handle Handle
) {
  Self->value.emplace(co_await std::move(Aw));
  Ctx->schedule(Handle);
}

// An lf::core::context_switcher that adapts a coro_util queue awaitable. libfork
// recognizes it through its await_transform and, on suspension, hands await_
// suspend the coroutine's submit_handle.
template <typename Awaitable> struct queue_op : result_storage<result_t<Awaitable>> {
  using R = result_t<Awaitable>;

  Awaitable aw;

  explicit queue_op(
    Awaitable Aw
  ) noexcept(std::is_nothrow_move_constructible_v<Awaitable>)
      : aw(std::move(Aw)) {}

  // Always suspend: the driver owns the co_await of the wrapped awaitable, so
  // the handle must be captured before any result is produced.
  bool await_ready() const noexcept { return false; }

  void await_suspend(lf::submit_handle Handle) noexcept {
    lf::worker_context* Ctx = lf::impl::tls::context();
    if constexpr (std::is_void_v<R>) {
      drive_void(std::move(aw), Ctx, Handle);
    } else {
      drive_value(std::move(aw), this, Ctx, Handle);
    }
  }

  R await_resume() noexcept {
    if constexpr (!std::is_void_v<R>) {
      return std::move(*this->value);
    }
  }
};

} // namespace libfork_detail

/// Adapts any coro_util queue/channel awaitable (q.pull(), q.push(x),
/// q.push_bulk(...)) so it can be co_awaited inside an lf::task<T> coroutine.
/// Returns an lf::core::context_switcher that yields the same value the wrapped
/// awaitable would. See the file header for usage and affinity notes.
template <typename Awaitable> libfork_detail::queue_op<Awaitable> lf_wrap(Awaitable Aw) {
  return libfork_detail::queue_op<Awaitable>{std::move(Aw)};
}

} // namespace coro_util
