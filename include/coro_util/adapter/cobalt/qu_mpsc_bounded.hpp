// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// Provides coro_util::qu_mpsc_bounded, the qu_mpsc_bounded_impl queue bound to
// the boost::cobalt continuation policy.

#include "policy.hpp"

#include "../../qu_mpsc_bounded.hpp"

namespace coro_util {

template <typename T, typename Config = coro_util::qu_mpsc_bounded_default_config>
using qu_mpsc_bounded =
  coro_util::qu_mpsc_bounded_impl<coro_util::detail::cobalt_continuation_policy, T, Config>;

} // namespace coro_util
