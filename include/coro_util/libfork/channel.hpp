// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// Provides coro_util::chan_tok and coro_util::make_channel, the channel access
// token bound to the generic inline continuation policy (executor affinity is
// restored by coro_util::lf_wrap() - see op.hpp).

#include "../inline/policy.hpp"

#include "../channel.hpp"

namespace coro_util {

template <typename T, typename Config = coro_util::chan_default_config>
using chan_tok =
  coro_util::impl::chan_tok<coro_util::impl::inline_continuation_policy, T, Config>;

/// Creates a new channel and returns an access token to it.
template <typename T, typename Config = coro_util::chan_default_config>
inline chan_tok<T, Config> make_channel() noexcept {
  return coro_util::impl::make_channel<
    coro_util::impl::inline_continuation_policy, T, Config>();
}

} // namespace coro_util
