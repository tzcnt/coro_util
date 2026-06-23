// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// Provides coro_util::qu_spsc_unbounded, the qu_spsc_unbounded_impl queue bound
// to the generic inline continuation policy (executor affinity is restored by
// coro_util::asio_wrap() - see op.hpp).

#include "../inline/policy.hpp"

#include "../../qu_spsc_unbounded.hpp"

namespace coro_util {

template <typename T, typename Config = coro_util::qu_spsc_unbounded_default_config>
using qu_spsc_unbounded = coro_util::qu_spsc_unbounded_impl<
  coro_util::detail::inline_continuation_policy, T, Config>;

} // namespace coro_util
