// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include "tmc/current.hpp"
#include "tmc/detail/concepts_awaitable.hpp"
#include "tmc/detail/thread_locals.hpp"
#include "tmc/ex_any.hpp"

#include <coroutine>
#include <type_traits>
#include <utility>

namespace coro_util {
namespace detail {

// Captures and restores the current task's executor and priority.
struct tmc_continuation_policy {
  struct state {
    tmc::ex_any* executor;
    size_t priority;
  };

  template <typename Promise>
  static state capture(std::coroutine_handle<Promise>) noexcept {
    return {tmc::current_executor(), tmc::current_priority()};
  }

  static void resume(state& State, std::coroutine_handle<> Continuation) noexcept {
    tmc::detail::post_checked(State.executor, std::move(Continuation), State.priority);
  }

  static void resume_inline(state&, std::coroutine_handle<> Continuation) noexcept {
    Continuation.resume();
  }
};

} // namespace detail
} // namespace coro_util

// The coro_util queue and channel awaitables are runtime-agnostic and carry no
// TMC awaitable tag. Each one exposes a queue_type back-reference and is awaited
// via operator co_await on an rvalue. Every TMC queue/channel adapter includes
// this header, so a single awaitable_traits specialization here covers all of
// them (aw_pull, aw_push, aw_push_bulk, aw_pull_zc, ...). Providing get_awaiter
// makes them "known" awaitables, so awaiting them inside a tmc::task resumes the
// task on its original executor and priority without wrapping it in a trampoline
// task.
namespace tmc::detail {
template <typename Awaitable>
concept coro_util_queue_awaitable = requires {
  typename Awaitable::queue_type;
} && requires(Awaitable&& Aw) { static_cast<Awaitable&&>(Aw).operator co_await(); };

template <coro_util_queue_awaitable Awaitable> struct awaitable_traits<Awaitable> {
  static constexpr configure_mode mode = WRAPPER;

  static decltype(auto) get_awaiter(Awaitable&& AwaitableValue) noexcept {
    return static_cast<Awaitable&&>(AwaitableValue).operator co_await();
  }

  using awaiter_type = decltype(get_awaiter(std::declval<Awaitable>()));
  using result_type =
    std::remove_reference_t<decltype(std::declval<awaiter_type&>().await_resume())>;
};
} // namespace tmc::detail
