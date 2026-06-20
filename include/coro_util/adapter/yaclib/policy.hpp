// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include "yaclib/exe/executor.hpp"
#include "yaclib/exe/job.hpp"

#include <coroutine>

namespace coro_util {
namespace detail {
// YACLib captures executor affinity in each task promise via a
// yaclib::detail::BaseCore::_executor pointer. A YACLib coroutine promise is also a
// yaclib::Job, so the standard way to reschedule a suspended coroutine onto its executor
// is to Submit that promise - which is what yaclib::On() does internally. We mirror that
// here, capturing both the executor and the promise-as-Job so resume() can reschedule
// without allocating a wrapper job.
struct yaclib_continuation_policy {
  struct state {
    yaclib::IExecutor* executor;
    yaclib::Job* job;
  };

  template <typename Promise>
  static state capture(std::coroutine_handle<Promise> Handle) noexcept {
    auto& promise = Handle.promise();
    return {promise._executor.Get(), &promise};
  }

  static void resume(state& State, std::coroutine_handle<>) noexcept {
    State.executor->Submit(*State.job);
  }

  static void resume_inline(state&, std::coroutine_handle<> Continuation) noexcept {
    Continuation.resume();
  }
};

} // namespace detail
} // namespace coro_util
