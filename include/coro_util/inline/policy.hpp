// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <coroutine>

namespace coro_util {

namespace detail {
struct inline_continuation_policy {
  struct state {};

  template <typename Promise>
  static state capture(std::coroutine_handle<Promise>) noexcept {
    return {};
  }

  static void resume(state&, std::coroutine_handle<> Continuation) noexcept {
    Continuation.resume();
  }

  static void resume_inline(state&, std::coroutine_handle<> Continuation) noexcept {
    Continuation.resume();
  }
};
} // namespace detail

} // namespace coro_util
