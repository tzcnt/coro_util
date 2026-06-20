// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <boost/asio/post.hpp>
#include <boost/cobalt/config.hpp>
#include <boost/cobalt/this_thread.hpp>

#include <coroutine>

namespace coro_util {
namespace detail {
// boost::cobalt is a coroutine layer over boost::asio. Every cobalt coroutine runs on an
// asio executor (boost::cobalt::executor == boost::asio::any_io_executor by default), and
// a task promise carries that executor via get_executor() - propagated from the awaiter
// exactly as cobalt's own task_receiver::awaitable::await_suspend does. To honor that
// affinity we capture the suspending coroutine's executor (falling back to the
// thread-local this_thread::get_executor() for promise types that expose no
// get_executor()) and reschedule the continuation onto it with asio::post - the standard
// "resume on this executor" primitive, which always defers to one of the executor's own
// threads rather than running inline on the waker.
struct cobalt_continuation_policy {
  struct state {
    boost::cobalt::executor executor;
  };

  template <typename Promise>
  static state capture(std::coroutine_handle<Promise> Handle) noexcept {
    if constexpr (requires(Promise& p) { p.get_executor(); })
      return {Handle.promise().get_executor()};
    else
      return {boost::cobalt::this_thread::get_executor()};
  }

  static void resume(state& State, std::coroutine_handle<> Continuation) noexcept {
    boost::asio::post(State.executor, [Continuation]() mutable {
      Continuation.resume();
    });
  }

  static void resume_inline(state&, std::coroutine_handle<> Continuation) noexcept {
    Continuation.resume();
  }
};

} // namespace detail
} // namespace coro_util
