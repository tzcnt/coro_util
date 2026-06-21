// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

// Provides coro_util::channel, an async MPMC unbounded linearizable queue.

// A channel can be created by coro_util::make_channel<T>().
// Producers enqueue values with post().
// Consumers retrieve values in FIFO order with co_await pull().
// If no values are available, the consumer will suspend until a value is ready.

// Access to the channel is through a token `chan_tok_impl` which shares ownership of
// the channel through reference counting, as well as holds a hazard pointer to
// the block in use. Any number of tokens can access the channel simultaneously,
// but access to a single token is not thread-safe. A token copy should be
// created (using the token copy constructor) for each thread or task that will
// access the channel concurrently.

// The hazard pointer scheme is loosely based on
// 'A wait-free queue as fast as fetch-and-add' by Yang & Mellor-Crummey
// https://dl.acm.org/doi/10.1145/2851141.2851168

#include "shared.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#if defined(__clang__)
#define CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN
#define CORO_UTIL_DISABLE_WARNING_PADDED_END
#elif defined(__GNUC__)
#define CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN
#define CORO_UTIL_DISABLE_WARNING_PADDED_END
#elif defined(_MSC_VER)
#define CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN                                           \
  _Pragma("warning(push)") _Pragma("warning(disable : 4324)")
#define CORO_UTIL_DISABLE_WARNING_PADDED_END _Pragma("warning(pop)")
#else
#define CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN
#define CORO_UTIL_DISABLE_WARNING_PADDED_END
#endif

namespace coro_util {

struct chan_default_config {
  /// The number of elements that can be stored in each block in the channel
  /// linked list.
  static inline constexpr size_t BlockSize = 256;

  /// If true, queue elements will be padded up to the next increment of 64
  /// bytes. This reduces false sharing between neighboring elements.
  /// If false, no padding will be applied.
  static inline constexpr bool ElementPadding = true;
};

/// Tokens share ownership of a channel by reference counting.
/// Access to the channel (from multiple tokens) is thread-safe,
/// but access to a single token from multiple threads is not.
/// To access the channel from multiple threads or tasks concurrently,
/// make a copy of the token for each (by using the copy constructor).
template <
  typename ContinuationPolicy, typename T,
  typename Config = coro_util::chan_default_config>
class chan_tok_impl;

/// Creates a new channel and returns an access token to it. Internal plumbing:
/// callers use the policy-bound make_channel wrapper provided by an adapter
/// header (e.g. adapter/tmc/channel.hpp).
namespace detail {
template <
  typename ContinuationPolicy, typename T,
  typename Config = coro_util::chan_default_config>
inline chan_tok_impl<ContinuationPolicy, T, Config> make_channel() noexcept;
} // namespace detail

template <
  typename ContinuationPolicy, typename T,
  typename Config = coro_util::chan_default_config>
class channel;
template <typename T> class chan_zc_scope;
template <typename T> class chan_try_pull_zc_scope;

namespace detail {
class tiny_lock {
  std::atomic_flag m_is_locked;

public:
  tiny_lock() noexcept { m_is_locked.clear(); }

  bool try_lock() noexcept {
    return !m_is_locked.test_and_set(std::memory_order_acquire);
  }

  void unlock() noexcept { m_is_locked.clear(std::memory_order_release); }
};

class [[nodiscard]] concurrent_access_scope {
  tiny_lock& lock;

public:
  explicit concurrent_access_scope(tiny_lock& Lock) noexcept : lock{Lock} {}
  ~concurrent_access_scope() noexcept { lock.unlock(); }

  concurrent_access_scope(const concurrent_access_scope&) = delete;
  concurrent_access_scope& operator=(const concurrent_access_scope&) = delete;
  concurrent_access_scope(concurrent_access_scope&&) = delete;
  concurrent_access_scope& operator=(concurrent_access_scope&&) = delete;
};

#ifndef NDEBUG
#define CORO_UTIL_CHANNEL_NO_CONCURRENT_ACCESS_LOCK                                      \
  coro_util::detail::tiny_lock no_concurrent_access_lock_;
#define CORO_UTIL_CHANNEL_ASSERT_NO_CONCURRENT_ACCESS()                                  \
  assert(                                                                                \
    no_concurrent_access_lock_.try_lock() &&                                             \
    "Concurrent access to a single chan_tok is not supported. You must create a "        \
    "separate chan_tok via new_token() or the copy constructor for each individual "     \
    "task that uses the channel."                                                        \
  );                                                                                     \
  coro_util::detail::concurrent_access_scope concurrent_access_check_(                   \
    no_concurrent_access_lock_                                                           \
  )
#else
#define CORO_UTIL_CHANNEL_NO_CONCURRENT_ACCESS_LOCK
#define CORO_UTIL_CHANNEL_ASSERT_NO_CONCURRENT_ACCESS()
#endif

// Memory order used for the hazard pointer protection stores (to
// active_offset) in get_write_ticket() / get_write_ticket_bulk() /
// get_read_ticket(). Each of these stores is immediately followed by a
// seq_cst RMW on the offset counter, and must be visible to the reclaimer's
// seq_cst revalidation load in keep_min() if the owner subsequently observes
// the pre-CAS value of its block pointer.
//
// The C++ memory model requires seq_cst on these stores: a seq_cst RMW on a
// different object does not order a preceding relaxed store against the
// reclaimer's loads. However, on x86 and ARM the compiled code is correct
// with a relaxed store:
// - on x86, a lock-prefixed RMW is a full fence;
// - on AArch64, the RMW's store-release orders the prior str, and the
// subsequent seq_cst (ldar) loads of the block pointer and of active_offset
// cannot be reordered before an earlier store-release (RCsc);
// - on ARMv7 / 32-bit ARM, the seq_cst RMW lowers to "dmb ish; ldrex/strex;
// dmb ish". The trailing dmb is the StoreLoad barrier we need. A seq_cst store
// here only adds an unnecessary dmb before the str. (Pre-v7
// single-core ARM has no observable reordering at all.)
// - Unaudited platforms get the formally correct seq_cst.
//
// LoongArch64 may also be usable with a relaxed store: clang lowers the
// seq_cst RMW to amadd_db.d. This is identical to acq_rel, which implies *_db
// atomics carry a full barrier like x86. This is pending further verification.
//
// Note: the seq_cst store in try_pull() is NOT covered by this and must
// remain unconditionally seq_cst - it is followed by a plain seq_cst load
// (not an RMW), which provides no StoreLoad ordering even on x86.
#if defined(__x86_64__) || defined(_M_AMD64) || defined(i386) || defined(__i386__) ||    \
  defined(__i386) || defined(_M_IX86) || defined(__arm__) || defined(_M_ARM) ||          \
  defined(_M_ARM64) || defined(__aarch64__) || defined(__ARM_ACLE)
inline constexpr std::memory_order hazptr_protect_order = std::memory_order_relaxed;
#else
inline constexpr std::memory_order hazptr_protect_order = std::memory_order_seq_cst;
#endif

// Hazard pointer type used internally by channel to track in-use blocks.
class alignas(CORO_UTIL_CACHE_LINE_SIZE) hazard_ptr {
  std::atomic<bool> owned;
  std::atomic<hazard_ptr*> next;
  std::atomic<size_t> active_offset;
  std::atomic<void*> write_block;
  std::atomic<void*> read_block;
  std::atomic<size_t> next_protect_write;
  std::atomic<size_t> next_protect_read;

  template <typename ContinuationPolicy, typename T, typename Config>
  friend class coro_util::channel;
  template <typename T> friend class coro_util::chan_zc_scope;
  template <typename T> friend class coro_util::chan_try_pull_zc_scope;

  static inline constexpr size_t InactiveHazptrOffset = static_cast<size_t>(1)
                                                        << (CORO_UTIL_PLATFORM_BITS - 2);

  inline void release_blocks() noexcept {
    // These elements may be read (by try_reclaim_block()) after
    // take_ownership() has been called, but before init() has been called.
    // These defaults ensure sane behavior.
    write_block.store(nullptr, std::memory_order_relaxed);
    read_block.store(nullptr, std::memory_order_relaxed);
  }

  inline hazard_ptr() noexcept {
    active_offset.store(InactiveHazptrOffset, std::memory_order_relaxed);
    release_blocks();
  }

  inline bool try_take_ownership() noexcept {
    bool expected = false;
    return owned.compare_exchange_strong(expected, true);
  }

  template <typename Pred, typename Func>
  void for_each_owned_hazptr(Pred pred, Func func) noexcept {
    hazard_ptr* curr = this;
    while (pred()) {
      hazard_ptr* n = curr->next.load(std::memory_order_acquire);
      bool is_owned = curr->owned.load(std::memory_order_relaxed);
      if (is_owned) {
        func(curr);
      }
      if (n == this) {
        break;
      }
      curr = n;
    }
  }

public:
  /// Returns the hazard pointer back to the hazard pointer freelist, so that
  /// it can be reused by another thread or task.
  inline void release_ownership() noexcept {
    release_blocks();
    owned.store(false);
  }
  CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN
};
CORO_UTIL_DISABLE_WARNING_PADDED_END

} // namespace detail

/// A zero-copy handle to an object in the channel's storage. The object is
/// exclusively available to this handle. When this handle is destroyed, the
/// channel object will be destroyed and the channel slot will be freed for
/// reuse. Returned by `co_await pull()`.
///
/// If the channel has been closed and is drained, `pull()` will resume with an
/// empty `chan_zc_scope` (`has_value()` / `operator bool()` returns false).
template <typename T> class chan_zc_scope {
  using hazard_ptr = coro_util::detail::hazard_ptr;
  hazard_ptr* haz_ptr;
  coro_util::detail::qu_storage<T>* data;
  size_t release_idx;

  template <typename ContinuationPolicy, typename U, typename Config>
  friend class coro_util::channel;
  template <typename ContinuationPolicy, typename U, typename Config>
  friend class coro_util::chan_tok_impl;

  chan_zc_scope(
    hazard_ptr* Haz, coro_util::detail::qu_storage<T>* Data, size_t ReleaseIdx
  ) noexcept
      : haz_ptr{Haz}, data{Data}, release_idx{ReleaseIdx} {}

  // Destroys the contained object (if any) and releases the channel slot.
  void release() noexcept {
    if (data != nullptr) {
      data->destroy();
      haz_ptr->active_offset.store(release_idx, std::memory_order_release);
      data = nullptr;
    }
  }

public:
  /// Constructs an empty scope. Evaluates to false when converted to bool.
  chan_zc_scope() noexcept : haz_ptr{nullptr}, data{nullptr}, release_idx{0} {}

  chan_zc_scope(const chan_zc_scope&) = delete;
  chan_zc_scope& operator=(const chan_zc_scope&) = delete;
  chan_zc_scope(chan_zc_scope&& Other) noexcept
      : haz_ptr{Other.haz_ptr}, data{Other.data}, release_idx{Other.release_idx} {
    Other.data = nullptr;
  }

  chan_zc_scope& operator=(chan_zc_scope&& Other) noexcept {
    if (this != &Other) {
      release();
      haz_ptr = Other.haz_ptr;
      data = Other.data;
      release_idx = Other.release_idx;
      Other.data = nullptr;
    }
    return *this;
  }

  /// Returns true if this scope holds a value from the channel.
  explicit operator bool() const noexcept { return data != nullptr; }

  /// Returns true if this scope holds a value from the channel.
  bool has_value() const noexcept { return data != nullptr; }

  /// Returns a reference to the object in the channel storage.
  /// Only valid to call if `has_value()` / `operator bool()` is true.
  T& value() noexcept { return data->value; }

  /// Returns a reference to the object in the channel storage.
  /// Only valid to call if `has_value()` / `operator bool()` is true.
  T& operator*() noexcept { return data->value; }

  /// Returns a pointer to the object in the channel storage.
  /// Only valid to call if `has_value()` / `operator bool()` is true.
  T* operator->() noexcept { return &data->value; }

  /// Destroys the object in the channel storage and releases the channel slot.
  ~chan_zc_scope() { release(); }
};

/// A zero-copy handle to an object in the channel's storage. The object is
/// exclusively available to this handle. When this handle is destroyed, the
/// channel object will be destroyed and the channel slot will be freed for
/// reuse. Returned by `try_pull()`.
///
/// The status of the pull is exposed via `status()`:
/// `qu_err::OK` if a value is held, `EMPTY` if no value was available, or
/// `CLOSED` if the channel has been closed and drained.
template <typename T> class chan_try_pull_zc_scope {
  using hazard_ptr = coro_util::detail::hazard_ptr;
  hazard_ptr* haz_ptr;
  coro_util::detail::qu_storage<T>* data;
  size_t release_idx;
  coro_util::qu_err err;

  template <typename ContinuationPolicy, typename U, typename Config>
  friend class coro_util::channel;
  template <typename ContinuationPolicy, typename U, typename Config>
  friend class coro_util::chan_tok_impl;

  // Holds a value dequeued from the channel (status OK).
  chan_try_pull_zc_scope(
    hazard_ptr* Haz, coro_util::detail::qu_storage<T>* Data, size_t ReleaseIdx
  ) noexcept
      : haz_ptr{Haz}, data{Data}, release_idx{ReleaseIdx}, err{coro_util::qu_err::OK} {}

  // Holds no value; status is EMPTY or CLOSED.
  explicit chan_try_pull_zc_scope(coro_util::qu_err Err) noexcept
      : haz_ptr{nullptr}, data{nullptr}, release_idx{0}, err{Err} {}

  // Destroys the contained object (if any) and releases the channel slot.
  void release() noexcept {
    if (data != nullptr) {
      data->destroy();
      haz_ptr->active_offset.store(release_idx, std::memory_order_release);
      data = nullptr;
    }
  }

public:
  /// Constructs an empty scope (status EMPTY). Evaluates to false when
  /// converted to bool.
  chan_try_pull_zc_scope() noexcept
      : haz_ptr{nullptr}, data{nullptr}, release_idx{0}, err{coro_util::qu_err::EMPTY} {}

  chan_try_pull_zc_scope(const chan_try_pull_zc_scope&) = delete;
  chan_try_pull_zc_scope& operator=(const chan_try_pull_zc_scope&) = delete;
  chan_try_pull_zc_scope(chan_try_pull_zc_scope&& Other) noexcept
      : haz_ptr{Other.haz_ptr}, data{Other.data}, release_idx{Other.release_idx},
        err{Other.err} {
    Other.data = nullptr;
    Other.err = coro_util::qu_err::EMPTY;
  }

  chan_try_pull_zc_scope& operator=(chan_try_pull_zc_scope&& Other) noexcept {
    if (this != &Other) {
      release();
      haz_ptr = Other.haz_ptr;
      data = Other.data;
      release_idx = Other.release_idx;
      err = Other.err;
      Other.data = nullptr;
      Other.err = coro_util::qu_err::EMPTY;
    }
    return *this;
  }

  /// Returns true if this scope holds a value from the channel (status == OK).
  explicit operator bool() const noexcept { return data != nullptr; }

  /// Returns true if this scope holds a value from the channel (status == OK).
  bool has_value() const noexcept { return data != nullptr; }

  /// Returns the status of this pull: OK, EMPTY, or CLOSED.
  coro_util::qu_err status() const noexcept { return err; }

  /// Returns a reference to the object in the channel storage.
  /// Only valid to call if `has_value()` / `operator bool()` is true.
  T& value() noexcept { return data->value; }

  /// Returns a reference to the object in the channel storage.
  /// Only valid to call if `has_value()` / `operator bool()` is true.
  T& operator*() noexcept { return data->value; }

  /// Returns a pointer to the object in the channel storage.
  /// Only valid to call if `has_value()` / `operator bool()` is true.
  T* operator->() noexcept { return &data->value; }

  /// Destroys the object in the channel storage and releases the channel slot.
  ~chan_try_pull_zc_scope() { release(); }
};

template <typename ContinuationPolicy, typename T, typename Config> class channel {
  static_assert(std::is_nothrow_destructible_v<T>);

  static inline constexpr size_t BlockSize = Config::BlockSize;
  static inline constexpr size_t BlockSizeMask = BlockSize - 1;
  static_assert(
    BlockSize && ((BlockSize & (BlockSize - 1)) == 0), "BlockSize must be a power of 2"
  );

  // Ensure that the subtraction of unsigned offsets always results in a value
  // that can be represented as a signed integer.
  static_assert(
    BlockSize <= (static_cast<size_t>(1) << (CORO_UTIL_PLATFORM_BITS - 1)),
    "BlockSize must not be larger than half the max value that can be "
    "represented by a platform word"
  );

  // An offset far enough forward that it won't protect anything for a very long
  // time, but close enough that it isn't considered "circular less than" 0.
  // On 32 bit this is only 1Gi elements. The worst case is that a
  // thread suspends for a very long time, and the queue processes 1Gi
  // elements and then cannot free any blocks until that thread wakes. This is
  // extremely unlikely, and not an error - it will just prevent block
  // reclamation. On 64 bit in practice this will never happen.
  static inline constexpr size_t InactiveHazptrOffset = static_cast<size_t>(1)
                                                        << (CORO_UTIL_PLATFORM_BITS - 2);

  friend chan_tok_impl<ContinuationPolicy, T, Config>;
  template <typename CPc, typename Tc, typename Cc>
  friend chan_tok_impl<CPc, Tc, Cc> detail::make_channel() noexcept;

public:
  class aw_pull;

  struct consumer_base {
    bool ok;
    typename ContinuationPolicy::state continuation_state;
    std::coroutine_handle<> continuation;
  };

  static_assert(std::is_default_constructible_v<typename ContinuationPolicy::state>);

private:
  // The flags value is combined with the consumer pointer: the upper bits
  // encode the consumer_base* (low 2 bits guaranteed 0 by alignment), allowing
  // both flags and consumer to be accessed at the same time.
  struct element {
    static inline constexpr uintptr_t DATA_BIT = static_cast<size_t>(1);
    static inline constexpr uintptr_t CONS_BIT = static_cast<size_t>(1) << 1;
    static inline constexpr uintptr_t BOTH_BITS = DATA_BIT | CONS_BIT;
    std::atomic<void*> flags;

    static_assert(alignof(consumer_base) >= 4);

  public:
    coro_util::detail::qu_storage<T> data;

    static constexpr size_t UNPADLEN =
      sizeof(std::atomic<void*>) + sizeof(coro_util::detail::qu_storage<T>);
    static constexpr size_t WANTLEN =
      (UNPADLEN + CORO_UTIL_CACHE_LINE_SIZE - 1) &
      static_cast<size_t>(0 - CORO_UTIL_CACHE_LINE_SIZE); // round up to
                                                          // CORO_UTIL_CACHE_LINE_SIZE
    static constexpr size_t PADLEN = UNPADLEN < WANTLEN ? (WANTLEN - UNPADLEN) : 999;

    struct empty {};
    using Padding =
      std::conditional_t<Config::ElementPadding && PADLEN != 999, char[PADLEN], empty>;
    CORO_UTIL_NO_UNIQUE_ADDRESS Padding pad;

    // If this returns false, data is ready and consumer should not wait.
    bool try_wait(consumer_base* Cons) noexcept {
      void* expected = nullptr;
      return flags.compare_exchange_strong(
        expected, static_cast<void*>(Cons), std::memory_order_acq_rel,
        std::memory_order_acquire
      );
    }

    // Sets the data ready flag,
    // or returns a consumer pointer if that consumer was already waiting.
    consumer_base* set_data_ready_or_get_waiting_consumer() noexcept {
      void* expected = nullptr;
      flags.compare_exchange_strong(
        expected, reinterpret_cast<void*>(DATA_BIT), std::memory_order_acq_rel,
        std::memory_order_acquire
      );
      return static_cast<consumer_base*>(expected);
    }

    // Used by consumers to notify close() that the consumer is not waiting
    // because it saw the closed flag and returned.
    // This does not need to be called during normal operation - only after the
    // closed flag is observed.
    void set_not_waiting() noexcept {
      flags.store(reinterpret_cast<void*>(BOTH_BITS), std::memory_order_release);
    }

    // Used by close() to find consumers that were already waiting on a slot
    // that will never receive data.
    consumer_base* spin_wait_for_waiting_consumer() noexcept {
      void* f = flags.load(std::memory_order_acquire);
      while (nullptr == f) {
        CORO_UTIL_CPU_PAUSE();
        f = flags.load(std::memory_order_acquire);
      }
      if (BOTH_BITS == reinterpret_cast<uintptr_t>(f)) {
        return nullptr;
      } else {
        return static_cast<consumer_base*>(f);
      }
    }

    bool is_data_waiting() noexcept {
      void* f = flags.load(std::memory_order_acquire);
      return DATA_BIT == reinterpret_cast<uintptr_t>(f);
    }

    void reset() noexcept { flags.store(nullptr, std::memory_order_relaxed); }
  };

  struct data_block {
    std::atomic<size_t> offset;
    std::atomic<data_block*> next;
    std::array<element, BlockSize> values;

    void reset_values() noexcept {
      for (size_t i = 0; i < BlockSize; ++i) {
        values[i].reset();
      }
    }

    data_block(size_t Offset) noexcept {
      offset.store(Offset, std::memory_order_relaxed);
      next.store(nullptr, std::memory_order_relaxed);
      reset_values();
    }

    data_block() noexcept : data_block(0) {}
  };

  static inline constexpr size_t WRITE_CLOSING_BIT = static_cast<size_t>(1);
  static inline constexpr size_t WRITE_CLOSED_BIT = static_cast<size_t>(1) << 1;
  // Infrequently modified values can share a cache line.
  // Written by close()
  CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN
  alignas(CORO_UTIL_CACHE_LINE_SIZE) std::atomic<size_t> closed;
  CORO_UTIL_DISABLE_WARNING_PADDED_END
  std::atomic<size_t> write_closed_at;

  // Written by get_hazard_ptr()
  using hazard_ptr = coro_util::detail::hazard_ptr;
  std::atomic<size_t> haz_ptr_counter;
  std::atomic<hazard_ptr*> hazard_ptr_list;

  // Written by set_*() configuration functions
  std::atomic<size_t> ReuseBlocks;
  char pad0[CORO_UTIL_CACHE_LINE_SIZE - sizeof(size_t)];
  std::atomic<size_t> write_offset;
  char pad1[CORO_UTIL_CACHE_LINE_SIZE - sizeof(size_t)];
  std::atomic<size_t> read_offset;
  char pad2[CORO_UTIL_CACHE_LINE_SIZE - sizeof(size_t)];

  // Blocks try_reclaim_blocks() and close().
  // Rarely blocks get_hazard_ptr() - if racing with try_reclaim_blocks().
  CORO_UTIL_DISABLE_WARNING_PADDED_BEGIN
  alignas(CORO_UTIL_CACHE_LINE_SIZE) std::mutex blocks_lock;
  CORO_UTIL_DISABLE_WARNING_PADDED_END
  std::atomic<size_t> reclaim_counter;
  std::atomic<data_block*> head_block;
  // Mirrors the offset of the most recently committed head_block, so that
  // get_hazard_ptr() never needs to dereference head_block (which may be a
  // stale pointer to a freed block when racing with try_reclaim_blocks()).
  // Only updated by committed (non-abandoned) reclaim operations; it may lag
  // head_block, which is safe - a low value only lowers the protection floor.
  std::atomic<size_t> head_offset;
  std::atomic<data_block*> tail_block;

  channel() noexcept {
    closed.store(0, std::memory_order_relaxed);
    write_closed_at.store(0, std::memory_order_relaxed);

    data_block* block = new data_block(0);
    head_block.store(block, std::memory_order_relaxed);
    head_offset.store(0, std::memory_order_relaxed);
    tail_block.store(block, std::memory_order_relaxed);
    read_offset.store(0, std::memory_order_relaxed);
    write_offset.store(0, std::memory_order_relaxed);

    ReuseBlocks.store(true, std::memory_order_relaxed);

    haz_ptr_counter.store(0, std::memory_order_relaxed);
    reclaim_counter.store(0, std::memory_order_relaxed);
    hazard_ptr* haz = new hazard_ptr;
    haz->next.store(haz, std::memory_order_relaxed);
    haz->owned.store(false, std::memory_order_relaxed);
    hazard_ptr_list.store(haz, std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
  }

  hazard_ptr* get_hazard_ptr_impl() noexcept {
    hazard_ptr* start = hazard_ptr_list.load(std::memory_order_relaxed);
    hazard_ptr* ptr = start;
    while (true) {
      hazard_ptr* next = ptr->next.load(std::memory_order_acquire);
      bool is_owned = ptr->owned.load(std::memory_order_relaxed);
      if ((is_owned == false) && ptr->try_take_ownership()) {
        break;
      }
      if (next == start) {
        hazard_ptr* newptr = new hazard_ptr;
        newptr->owned.store(true, std::memory_order_relaxed);
        do {
          newptr->next.store(next, std::memory_order_release);
        } while (!ptr->next.compare_exchange_strong(
          next, newptr, std::memory_order_acq_rel, std::memory_order_acquire
        ));
        ptr = newptr;
        break;
      }
      ptr = next;
    }
    return ptr;
  }

  static inline bool circular_less_than(size_t a, size_t b) noexcept {
    return a - b > (static_cast<size_t>(1) << (CORO_UTIL_PLATFORM_BITS - 1));
  }

  // Load src and move it into dst if src < dst.
  // seq_cst is needed so that this load is ordered (in the seq_cst total
  // order) after the seq_cst CAS in try_advance_hazptr_block(). Paired with
  // the protection stores to active_offset in get_*_ticket() / try_pull()
  // (see hazptr_protect_order), this guarantees that if a token observed the
  // pre-CAS block pointer, the revalidation here will observe that token's
  // active_offset protection.
  static inline void keep_min(size_t& Dst, std::atomic<size_t> const& Src) noexcept {
    size_t val = Src.load(std::memory_order_seq_cst);
    if (circular_less_than(val, Dst)) {
      Dst = val;
    }
  }

  // Move src into dst if src < dst.
  static inline void keep_min(size_t& Dst, size_t Src) noexcept {
    if (circular_less_than(Src, Dst)) {
      Dst = Src;
    }
  }

  // Advances DstBlock to be equal to NewHead. Possibly reduces MinProtect if
  // DstBlock was already updated by its owning task.
  static inline void try_advance_hazptr_block(
    std::atomic<void*>& DstBlock, size_t& MinProtected, data_block* NewHead,
    std::atomic<size_t> const& HazardOffset
  ) noexcept {
    void* block = DstBlock.load(std::memory_order_acquire);
    if (block == nullptr) {
      // A newly owned hazptr. It will reload the value of head after this
      // reclaim operation completes, or cause the entire reclaim operation to
      // be abandoned. In either case, we don't need to update it here.
      // May also be a newly released hazptr, in which case we don't want to
      // overwrite the value of block either.
      return;
    }
    if (circular_less_than(
          static_cast<data_block*>(block)->offset.load(std::memory_order_relaxed),
          NewHead->offset.load(std::memory_order_relaxed)
        )) {
      if (!DstBlock.compare_exchange_strong(
            block, static_cast<void*>(NewHead), std::memory_order_seq_cst
          )) {
        if (block == nullptr) {
          // A newly released hazptr.
          return;
        }
        // If this hazptr updated its own block, but the updated block is
        // still earlier than the new head, then we cannot free that block.
        keep_min(
          MinProtected,
          static_cast<data_block*>(block)->offset.load(std::memory_order_relaxed)
        );
      }
      // Reload hazptr after trying to modify block to ensure that if it was
      // written, its value is seen.
      keep_min(MinProtected, HazardOffset);
    }
  }

  // Starting from OldHead, advance forward through the block list, stopping at
  // the first block that is protected by a hazard pointer. This block is
  // returned to become the NewHead. If OldHead is protected, then it will be
  // returned unchanged, and no blocks can be reclaimed.
  data_block*
  try_advance_head(hazard_ptr* Haz, data_block* OldHead, size_t ProtectIdx) noexcept {
    // In the current implementation, this is called only from consumers.
    // Therefore, this token's hazptr will be active, and protecting read_block.
    // However, if producers are lagging behind, and no producer is currently
    // active, write_block would not be protected. Therefore, write_offset
    // should be passed to ProtectIdx to cover this scenario.
    ProtectIdx = ProtectIdx & ~BlockSizeMask; // round down to block index

    // Find the lowest offset that is protected by ProtectIdx or any hazptr.
    size_t oldOff = OldHead->offset.load(std::memory_order_relaxed);
    Haz->for_each_owned_hazptr(
      [&]() { return circular_less_than(oldOff, ProtectIdx); },
      [&](hazard_ptr* curr) { keep_min(ProtectIdx, curr->active_offset); }
    );

    // If head block is protected, nothing can be reclaimed.
    if (circular_less_than(ProtectIdx, 1 + oldOff)) {
      return OldHead;
    }

    // Find the block associated with this offset.
    data_block* newHead = OldHead;
    while (
      circular_less_than(newHead->offset.load(std::memory_order_relaxed), ProtectIdx)
    ) {
      newHead = newHead->next.load(std::memory_order_acquire);
    }

    // Then update all hazptrs to be at this block or later.
    Haz->for_each_owned_hazptr(
      [&]() { return circular_less_than(oldOff, ProtectIdx); },
      [&](hazard_ptr* curr) {
        try_advance_hazptr_block(
          curr->write_block, ProtectIdx, newHead, curr->active_offset
        );
        try_advance_hazptr_block(
          curr->read_block, ProtectIdx, newHead, curr->active_offset
        );
      }
    );

    // ProtectIdx may have been reduced by the double-check in
    // try_advance_block. If so, reduce newHead as well.
    if (circular_less_than(ProtectIdx, newHead->offset.load(std::memory_order_relaxed))) {
      newHead = OldHead;
      while (
        circular_less_than(newHead->offset.load(std::memory_order_relaxed), ProtectIdx)
      ) {
        newHead = newHead->next;
      }
    }

#ifndef NDEBUG
    assert(circular_less_than(
      newHead->offset.load(std::memory_order_relaxed),
      1 + read_offset.load(std::memory_order_acquire)
    ));
    assert(circular_less_than(
      newHead->offset.load(std::memory_order_relaxed),
      1 + write_offset.load(std::memory_order_acquire)
    ));
#endif
    return newHead;
  }

  void reclaim_blocks(data_block* OldHead, data_block* NewHead) noexcept {
    if (!ReuseBlocks.load(std::memory_order_relaxed)) {
      while (OldHead != NewHead) {
        data_block* next = OldHead->next.load(std::memory_order_relaxed);
        delete OldHead;
        OldHead = next;
      }
    } else {
      // Reset blocks and move them to the tail of the list in groups of 4.
      while (true) {
        std::array<data_block*, 4> unlinked;
        size_t unlinkedCount = 0;
        for (; unlinkedCount < unlinked.size(); ++unlinkedCount) {
          if (OldHead == NewHead) {
            break;
          }
          unlinked[unlinkedCount] = OldHead;
          OldHead = OldHead->next.load(std::memory_order_acquire);
        }
        if (unlinkedCount == 0) {
          break;
        }

        for (size_t i = 0; i < unlinkedCount; ++i) {
          unlinked[i]->reset_values();
        }

        data_block* tailBlock = tail_block.load(std::memory_order_acquire);
        data_block* next = tailBlock->next.load(std::memory_order_acquire);

        // Iterate forward in case tailBlock is part of unlinked.
        while (next != nullptr) {
          tailBlock = next;
          next = tailBlock->next.load(std::memory_order_acquire);
        }
        // Actually unlink the blocks from the head of the queue.
        // They stay linked to each other.
        unlinked[unlinkedCount - 1]->next.store(nullptr, std::memory_order_release);

        while (true) {
          // Update their offsets to the end of the queue.
          size_t boff = tailBlock->offset.load(std::memory_order_relaxed) + BlockSize;
          for (size_t i = 0; i < unlinkedCount; ++i) {
            unlinked[i]->offset.store(boff, std::memory_order_relaxed);
            boff += BlockSize;
          }

          // Re-link the tail of the queue to the head of the unlinked blocks.
          if (tailBlock->next.compare_exchange_strong(
                next, unlinked[0], std::memory_order_acq_rel, std::memory_order_acquire
              )) {
            break;
          }

          // Tail was out of date, find the new tail.
          while (next != nullptr) {
            tailBlock = next;
            next = tailBlock->next.load(std::memory_order_acquire);
          }
        }

        tail_block.store(unlinked[unlinkedCount - 1]);
      }
    }
  }

  // Access to this function must be externally synchronized (via blocks_lock).
  // Blocks that are not protected by a hazard pointer will be reclaimed, and
  // head_block will be advanced to the first protected block.
  void try_reclaim_blocks(hazard_ptr* Haz, size_t ProtectIdx) noexcept {
    data_block* oldHead = head_block.load(std::memory_order_acquire);
    // reclaim_counter and haz_ptr_counter behave as a split lock shared with
    // get_hazard_ptr(). If both operations run at the same time, this will
    // abandon its operation before the final stage.
    size_t hazptrCount = haz_ptr_counter.load(std::memory_order_acquire);

    // Perform the private stage of the operation.
    data_block* newHead = try_advance_head(Haz, oldHead, ProtectIdx);
    if (newHead == oldHead) {
      return;
    }
    head_block.store(newHead, std::memory_order_release);

    // Signal to get_hazard_ptr() that we updated head_block.
    reclaim_counter.fetch_add(1, std::memory_order_seq_cst);

    // Check if get_hazard_ptr() was running.
    size_t hazptrCheck = haz_ptr_counter.load(std::memory_order_seq_cst);
    if (hazptrCount != hazptrCheck) {
      // A hazard pointer was acquired during try_advance_head().
      // It may have an outdated value of head. Our options are to run
      // try_advance_head() again, or just abandon (rollback) the operation. For
      // now, I've chosen to abandon the operation. This will run again when the
      // next block is allocated.
      head_block.store(oldHead, std::memory_order_release);
      return;
    }
    // Publish the committed head's offset. This must be stored after the
    // head_block store and only on the committed path: a reader that
    // acquires this offset is then guaranteed to observe a head_block at
    // least as new as newHead, so the offset it reads can never exceed the
    // offset of the block pointer it reads (see get_hazard_ptr()). An
    // abandoned reclaim leaves head_offset stale-low, which is safe.
    head_offset.store(
      newHead->offset.load(std::memory_order_relaxed), std::memory_order_release
    );
    reclaim_blocks(oldHead, newHead);
  }

  // Given idx and a starting block, advance it until the block containing idx
  // is found.
  static inline data_block* find_block(data_block* Block, size_t Idx) noexcept {
    size_t offset = Block->offset.load(std::memory_order_relaxed);
    size_t targetOffset = Idx & ~BlockSizeMask;
    // Find or allocate the associated block
    while (offset != targetOffset) {
      data_block* next = Block->next.load(std::memory_order_acquire);
      if (next == nullptr) {
        data_block* newBlock = new data_block(offset + BlockSize);
        if (Block->next.compare_exchange_strong(
              next, newBlock, std::memory_order_acq_rel, std::memory_order_acquire
            )) {
          next = newBlock;
        } else {
          delete newBlock;
        }
      }
      Block = next;
      offset += BlockSize;
      assert(Block->offset.load(std::memory_order_relaxed) == offset);
    }

    assert(
      Idx >= Block->offset.load(std::memory_order_relaxed) &&
      Idx <= Block->offset.load(std::memory_order_relaxed) + BlockSize - 1
    );
    return Block;
  }

  // Returns true if the channel is closed AND index Idx is at/after the close
  // point (write_closed_at), so it will never be serviced.
  //
  // Regarding memory order:
  // - Read-side slot claims (get_read_ticket) MUST pass seq_cst. Lost-wakeup
  //   avoidance there is a Dekker-style store-load pair: close() stores
  //   `closed` then loads read_offset, while the consumer RMWs read_offset then
  //   loads `closed` here. close() only *loads* read_offset (it does not RMW
  //   it), so there is no release sequence to ride; the no-mutual-miss
  //   guarantee from all 4 of these ops, including this load, being seq_cst.
  // - Write-side claims may pass acquire: close() does an RMW fetch_add on
  //   write_offset, so a writer's seq_cst fetch_add synchronizes-with it via
  //   the release sequence, forcing visibility of `closed` independently of
  //   this load's order. acquire is still needed so the relaxed write_closed_at
  //   load below sees the value close() stored before its release of `closed`.
  // - Non-blocking try_pull may pass acquire: it never blocks, so eventual
  //   visibility is sufficient.
  template <std::memory_order Order> bool is_closed_past(size_t Idx) const noexcept {
    auto closedState = closed.load(Order);
    if (0 == closedState) [[likely]] {
      return false;
    }
    // Wait for the write_closed_at index to become available.
    while (0 == (closedState & WRITE_CLOSED_BIT)) {
      CORO_UTIL_CPU_PAUSE();
      closedState = closed.load(std::memory_order_acquire);
    }
    return circular_less_than(write_closed_at.load(std::memory_order_relaxed), 1 + Idx);
  }

  // Idx will be initialized by this function
  element* get_write_ticket(hazard_ptr* Haz, size_t& Idx) noexcept {
    size_t actOff = Haz->next_protect_write.load(std::memory_order_relaxed);
    Haz->active_offset.store(actOff, coro_util::detail::hazptr_protect_order);

    // seq_cst is needed here to create a StoreLoad barrier between setting
    // hazptr and loading the block
    Idx = write_offset.fetch_add(1, std::memory_order_seq_cst);
    data_block* block =
      static_cast<data_block*>(Haz->write_block.load(std::memory_order_seq_cst));

    [[maybe_unused]] size_t boff = block->offset.load(std::memory_order_relaxed);
    assert(circular_less_than(actOff, 1 + Idx));
    assert(circular_less_than(boff, 1 + Idx));

    // close() will set `closed` before incrementing write_offset.
    // Thus we are guaranteed to see it if we acquire offset first (our Idx will
    // be past write_closed_at).
    //
    // We may also see it earlier than that, in which case we should not return
    // early (our Idx is less than write_closed_at).
    if (is_closed_past<std::memory_order_acquire>(Idx)) [[unlikely]] {
      // Nothing will be written; release the hazard pointer so that this
      // token doesn't block reclamation while it is idle.
      Haz->active_offset.store(Idx + InactiveHazptrOffset, std::memory_order_release);
      return nullptr;
    }
    block = find_block(block, Idx);
    // Update last known block.
    Haz->write_block.store(block, std::memory_order_release);
    Haz->next_protect_write.store(boff, std::memory_order_relaxed);
    element* elem = &block->values[Idx & BlockSizeMask];
    return elem;
  }

  // StartIdx and EndIdx will be initialized by this function
  data_block* get_write_ticket_bulk(
    hazard_ptr* Haz, size_t Count, size_t& StartIdx, size_t& EndIdx
  ) noexcept {
    size_t actOff = Haz->next_protect_write.load(std::memory_order_relaxed);
    Haz->active_offset.store(actOff, coro_util::detail::hazptr_protect_order);

    // seq_cst is needed here to create a StoreLoad barrier between setting
    // hazptr and loading the block
    StartIdx = write_offset.fetch_add(Count, std::memory_order_seq_cst);
    EndIdx = StartIdx + Count;
    data_block* block =
      static_cast<data_block*>(Haz->write_block.load(std::memory_order_seq_cst));

    [[maybe_unused]] size_t boff = block->offset.load(std::memory_order_relaxed);
    assert(circular_less_than(actOff, 1 + StartIdx));
    assert(circular_less_than(boff, 1 + StartIdx));

    // close() will set `closed` before incrementing write_offset.
    // Thus we are guaranteed to see it if we acquire offset first (our Idx will
    // be past write_closed_at).
    //
    // We may also see it earlier than that, in which case we should not return
    // early (our Idx is less than write_closed_at).
    if (is_closed_past<std::memory_order_acquire>(StartIdx)) [[unlikely]] {
      // Nothing will be written; release the hazard pointer so that this
      // token doesn't block reclamation while it is idle.
      Haz->active_offset.store(EndIdx + InactiveHazptrOffset, std::memory_order_release);
      return nullptr;
    }

    // Ensure all blocks for the operation are allocated and available.
    data_block* startBlock = find_block(block, StartIdx);

    data_block* protectBlock;
    if (StartIdx != EndIdx) [[likely]] {
      data_block* endBlock = find_block(startBlock, EndIdx - 1);
      protectBlock = endBlock;
    } else {
      // User passed an empty range, or Count == 0
      protectBlock = startBlock;
    }
    // Update last known block.
    Haz->write_block.store(protectBlock, std::memory_order_release);
    Haz->next_protect_write.store(
      protectBlock->offset.load(std::memory_order_relaxed), std::memory_order_relaxed
    );
    return startBlock;
  }

  // Idx will be initialized by this function
  element* get_read_ticket(hazard_ptr* Haz, size_t& Idx) noexcept {
    size_t actOff = Haz->next_protect_read.load(std::memory_order_relaxed);
    Haz->active_offset.store(actOff, coro_util::detail::hazptr_protect_order);

    // seq_cst is needed here to create a StoreLoad barrier between setting
    // hazptr and loading the block
    Idx = read_offset.fetch_add(1, std::memory_order_seq_cst);
    data_block* block =
      static_cast<data_block*>(Haz->read_block.load(std::memory_order_seq_cst));

    [[maybe_unused]] size_t boff = block->offset.load(std::memory_order_relaxed);
    assert(circular_less_than(actOff, 1 + Idx));
    assert(circular_less_than(boff, 1 + Idx));

    // Lost-wakeup avoidance is a Dekker-style store-load on two locations:
    // - close():  store `closed` (seq_cst), then load read_offset (seq_cst)
    // - consumer: RMW read_offset (seq_cst), then load `closed` (seq_cst)
    if (is_closed_past<std::memory_order_seq_cst>(Idx)) [[unlikely]] {
      // If closed, continue draining until the channel is empty.
      // After channel is empty, we still need to mark each element as
      // finished. This is a side effect of using fetch_add - we are still
      // consuming indexes even if they aren't used.
      block = find_block(block, Idx);
      element* elem = &block->values[Idx & BlockSizeMask];
      elem->set_not_waiting();
      // Also release the hazard pointer now (nothing else to read)
      Haz->active_offset.store(Idx + InactiveHazptrOffset, std::memory_order_release);
      return nullptr;
    }
    block = find_block(block, Idx);
    // Update last known block.
    // Note that if hazptr was to an older block, that block will still be
    // protected (by active_offset). This prevents a channel consisting of a
    // single block from trying to unlink/link that block to itself.
    Haz->read_block.store(block, std::memory_order_release);
    Haz->next_protect_read.store(boff, std::memory_order_relaxed);
    // Try to reclaim old blocks. Checking for index 1 ensures that at least
    // this token's hazptr will already be advanced to the new block.
    // Only consumers participate in reclamation and only 1 consumer at a time.
    if ((Idx & BlockSizeMask) == 1 && blocks_lock.try_lock()) {
      size_t protectIdx = write_offset.load(std::memory_order_acquire);
      try_reclaim_blocks(Haz, protectIdx);
      blocks_lock.unlock();
    }
    element* elem = &block->values[Idx & BlockSizeMask];
    return elem;
  }

  static void resume_consumer(consumer_base* Cons) noexcept {
    ContinuationPolicy::resume(Cons->continuation_state, Cons->continuation);
  }

  template <typename... Args>
  void write_element(element* Elem, Args&&... ConstructArgs) noexcept {
    // Always construct data in-place in channel storage (zero-copy)
    Elem->data.emplace(std::forward<Args>(ConstructArgs)...);

    // Finalize transaction
    auto cons = Elem->set_data_ready_or_get_waiting_consumer();
    if (cons != nullptr) {
      // Resume them so they can read from channel storage.
      resume_consumer(cons);
    }
  }

  // HeadOff is passed in (from head_offset) rather than read from
  // head->offset: head must not be dereferenced here, as it may be a stale
  // pointer to a freed block if this races with try_reclaim_blocks(). The
  // counter handshake in get_hazard_ptr() detects such a race and retries,
  // but only after this function has already run.
  void init_haz_ptr(hazard_ptr* haz, data_block* head, size_t HeadOff) noexcept {
    haz->next_protect_write.store(HeadOff, std::memory_order_relaxed);
    haz->next_protect_read.store(HeadOff, std::memory_order_relaxed);
    haz->active_offset.store(HeadOff + InactiveHazptrOffset, std::memory_order_relaxed);
    haz->read_block.store(head, std::memory_order_relaxed);
    haz->write_block.store(head, std::memory_order_relaxed);
  }

public:
  // Gets a hazard pointer from the list, and takes ownership of it.
  hazard_ptr* get_hazard_ptr() noexcept {
    // reclaim_counter and haz_ptr_counter behave as a split lock shared with
    // try_reclaim_blocks(). If both operations run at the same time, we may see
    // an outdated value of head, and will need to reload head.
    size_t reclaimCount = reclaim_counter.load(std::memory_order_acquire);

    // Perform the private stage of the operation.
    hazard_ptr* ptr = get_hazard_ptr_impl();

    // Reload head_block until try_reclaim_blocks was not running.
    size_t reclaimCheck;
    do {
      reclaimCheck = reclaimCount;
      // head_offset must be loaded before head_block. It is stored (release)
      // only after the head_block store of a committed reclaim, so acquiring
      // it first guarantees the subsequent head_block load observes a block
      // at least as new - the offset read here can never exceed the offset
      // of the block read below.
      size_t headOff = head_offset.load(std::memory_order_acquire);
      data_block* head = head_block.load(std::memory_order_acquire);
      init_haz_ptr(ptr, head, headOff);
      // Signal to try_reclaim_blocks() that we read the value of head_block.
      haz_ptr_counter.fetch_add(1, std::memory_order_seq_cst);
      // Check if try_reclaim_blocks() was running (again)
      reclaimCount = reclaim_counter.load(std::memory_order_seq_cst);
    } while (reclaimCount != reclaimCheck);
    return ptr;
  }

  template <typename... Args>
  bool post(hazard_ptr* Haz, Args&&... ConstructArgs) noexcept {
    // Get write ticket and associated block, protected by hazptr.
    size_t idx;
    element* elem = get_write_ticket(Haz, idx);
    if (elem == nullptr) [[unlikely]] {
      return false;
    }

    // Construct the data in-place / wake any waiting consumers
    write_element(elem, std::forward<Args>(ConstructArgs)...);

    // Then release the hazard pointer
    Haz->active_offset.store(idx + InactiveHazptrOffset, std::memory_order_release);

    return true;
  }

  template <typename It>
  bool post_bulk(hazard_ptr* Haz, It&& Items, size_t Count) noexcept {
    // Get write ticket and associated block, protected by hazptr.
    size_t startIdx, endIdx;
    data_block* block = get_write_ticket_bulk(Haz, Count, startIdx, endIdx);
    if (block == nullptr) [[unlikely]] {
      return false;
    }

    size_t idx = startIdx;
    while (idx != endIdx) {
      element* elem = &block->values[idx & BlockSizeMask];
      // Store the data / wake any waiting consumers
      CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_BEGIN
      write_element(elem, std::move(*Items));
      CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_END
      ++Items;
      ++idx;
      if ((idx & BlockSizeMask) == 0) {
        block = block->next.load(std::memory_order_acquire);
        // all blocks should have been preallocated for [startIdx, endIdx)
        assert(block != nullptr || !circular_less_than(idx, endIdx));
      }
    }

    // Then release the hazard pointer
    Haz->active_offset.store(endIdx + InactiveHazptrOffset, std::memory_order_release);

    return true;
  }

  class aw_pull final {
  public:
    // Back-reference to the owning channel. This lets a library adapter
    // recognize channel pull awaitables and specialize its awaitable traits for
    // them, without the channel itself depending on that library. The member is
    // named queue_type to match the queue awaitables so a single adapter
    // specialization can cover both.
    using queue_type = channel;

  private:
    channel& chan;
    hazard_ptr* haz_ptr;

    friend chan_tok_impl<ContinuationPolicy, T, Config>;

    aw_pull(channel& Chan, hazard_ptr* Haz) noexcept : chan(Chan), haz_ptr{Haz} {}

    struct aw_pull_impl final {
      consumer_base base;
      aw_pull& parent;
      element* elem;
      size_t release_idx;

      aw_pull_impl(aw_pull& Parent) noexcept : base{true, {}, nullptr}, parent{Parent} {}

      bool await_ready() noexcept {
        // Get read ticket and associated block, protected by hazptr.
        size_t idx;
        elem = parent.chan.get_read_ticket(parent.haz_ptr, idx);
        if (elem == nullptr) [[unlikely]] {
          // The queue is closed and drained.
          base.ok = false;
          return true;
        }
        release_idx = idx + InactiveHazptrOffset;

        if (elem->is_data_waiting()) {
          // Data is already ready in channel storage (zero-copy).
          return true;
        }

        // If we suspend, hold on to the hazard pointer to keep the block alive
        return false;
      }

      template <typename Promise>
      bool await_suspend(std::coroutine_handle<Promise> Outer) noexcept {
        base.continuation_state = ContinuationPolicy::capture(Outer);
        base.continuation = std::coroutine_handle<>::from_address(Outer.address());
        if (!elem->try_wait(&base)) {
          // data became ready during our RMW cycle
          return false;
        }
        return true;
      }

      // Returns a chan_zc_scope holding the dequeued value, or an empty scope
      // (has_value() == false) if the channel was closed and drained.
      chan_zc_scope<T> await_resume() noexcept {
        if (base.ok) {
          return chan_zc_scope<T>(parent.haz_ptr, &elem->data, release_idx);
        } else {
          return chan_zc_scope<T>();
        }
      }
    };

  public:
    aw_pull_impl operator co_await() && noexcept { return aw_pull_impl(*this); }
  };

  chan_try_pull_zc_scope<T> try_pull(hazard_ptr* Haz) {
    // Get read ticket and associated block, protected by hazptr.
    size_t actOff = Haz->next_protect_read.load(std::memory_order_relaxed);

    // seq_cst is needed here to create a StoreLoad barrier between setting
    // hazptr and loading the block
    Haz->active_offset.store(actOff, std::memory_order_seq_cst);

    size_t Idx = read_offset.load(std::memory_order_seq_cst);
    while (true) {
      auto woff = write_offset.load(std::memory_order_relaxed);
      // If woff <= roff, the queue appears empty.
      if (circular_less_than(woff, Idx + 1)) {
        // If closed, continue draining until the channel is empty.
        if (is_closed_past<std::memory_order_acquire>(Idx)) [[unlikely]] {
          Haz->active_offset.store(Idx + InactiveHazptrOffset, std::memory_order_release);
          return chan_try_pull_zc_scope<T>(coro_util::qu_err::CLOSED);
        }
        // Release the hazard pointer so that this token doesn't block
        // reclamation while it is idle.
        Haz->active_offset.store(Idx + InactiveHazptrOffset, std::memory_order_release);
        return chan_try_pull_zc_scope<T>(coro_util::qu_err::EMPTY);
      }
      // Queue appears non-empty. See if data is ready for consumption at our
      // speculative Idx.
      data_block* block =
        static_cast<data_block*>(Haz->read_block.load(std::memory_order_seq_cst));

      [[maybe_unused]] size_t boff = block->offset.load(std::memory_order_relaxed);
      assert(circular_less_than(actOff, 1 + Idx));
      assert(circular_less_than(boff, 1 + Idx));

      block = find_block(block, Idx);

      element* elem = &block->values[Idx & BlockSizeMask];
      if (elem->is_data_waiting()) {
        if (read_offset.compare_exchange_strong(
              Idx, Idx + 1, std::memory_order_seq_cst, std::memory_order_seq_cst
            )) {
          // Update last known block.
          // Note that if hazptr was to an older block, that block will still be
          // protected (by active_offset). This prevents a channel consisting of
          // a single block from trying to unlink/link that block to itself.
          Haz->read_block.store(block, std::memory_order_release);
          Haz->next_protect_read.store(boff, std::memory_order_relaxed);
          // Try to reclaim old blocks. Checking for index 1 ensures that at
          // least this token's hazptr will already be advanced to the new
          // block. Only consumers participate in reclamation and only 1
          // consumer at a time.
          if ((Idx & BlockSizeMask) == 1 && blocks_lock.try_lock()) {
            try_reclaim_blocks(Haz, woff);
            blocks_lock.unlock();
          }

          // Zero-copy: the value stays in channel storage and the hazard
          // pointer keeps protecting the block. The returned scope destroys the
          // value and releases the hazard pointer (active_offset) when it is
          // itself destroyed.
          return chan_try_pull_zc_scope<T>(Haz, &elem->data, Idx + InactiveHazptrOffset);
        }
      } else {
        auto oldIdx = Idx;
        Idx = read_offset.load(std::memory_order_seq_cst);
        if (Idx != oldIdx) {
          // Another consumer claimed the slot at oldIdx before we could, so
          // retry with the new Idx.
          continue;
        }
        // No data is ready at Idx and no other consumer has claimed it.
        // The queue may appear non-empty based on indexes alone because close()
        // and failed post() calls after close continue to increment
        // write_offset; those slots will never contain data. Check for that
        // here, or a polling consumer would see EMPTY forever on a closed and
        // fully-drained channel.
        // If Idx < write_closed_at, a producer claimed this slot before
        // close() and its data is still in flight; report EMPTY below so
        // the consumer retries. Otherwise no data can ever arrive at or
        // after Idx, and everything before Idx has been consumed.
        if (is_closed_past<std::memory_order_acquire>(Idx)) [[unlikely]] {
          Haz->active_offset.store(Idx + InactiveHazptrOffset, std::memory_order_release);
          return chan_try_pull_zc_scope<T>(coro_util::qu_err::CLOSED);
        }
        // Release the hazard pointer so that this token doesn't block
        // reclamation while it is idle.
        Haz->active_offset.store(Idx + InactiveHazptrOffset, std::memory_order_release);
        return chan_try_pull_zc_scope<T>(coro_util::qu_err::EMPTY);
      }
    }
  }

  // called by close()
  void wake_waiting_consumers(size_t StartIdx, size_t EndIdx) noexcept {
    if (!circular_less_than(StartIdx, EndIdx)) {
      return;
    }

    data_block* block = head_block.load(std::memory_order_acquire);
    size_t idx = StartIdx;
    block = find_block(block, idx);
    while (circular_less_than(idx, EndIdx)) {
      auto cons = block->values[idx & BlockSizeMask].spin_wait_for_waiting_consumer();
      if (cons != nullptr) {
        cons->ok = false;
        resume_consumer(cons);
      }

      ++idx;
      if ((idx & BlockSizeMask) == 0 && circular_less_than(idx, EndIdx)) {
        block = find_block(block, idx);
      }
    }
  }

  void close() noexcept {
    std::scoped_lock<std::mutex> lg(blocks_lock);
    if (0 != closed.load(std::memory_order_relaxed)) {
      return;
    }
    size_t woff = write_offset.load(std::memory_order_seq_cst);
    // Setting this to a distant-but-greater value before setting closed
    // prevents consumers from exiting too early.
    write_closed_at.store(woff + InactiveHazptrOffset, std::memory_order_seq_cst);

    closed.store(WRITE_CLOSING_BIT, std::memory_order_seq_cst);

    // Now mark the real closed_at index. Past this index, producers are
    // guaranteed to not produce. Prior to this index, producers may still be
    // in flight, but those slots were already reserved before close() claimed
    // the sentinel and will still be produced.
    woff = write_offset.fetch_add(1, std::memory_order_seq_cst);
    write_closed_at.store(woff, std::memory_order_seq_cst);
    closed.store(WRITE_CLOSING_BIT | WRITE_CLOSED_BIT, std::memory_order_seq_cst);

    // Readers that already claimed slots >= write_closed_at will never
    // receive data. Wake them now.
    wake_waiting_consumers(woff, read_offset.load(std::memory_order_seq_cst));
  }

  ~channel() {
    {
      // Since tokens share ownership of channel, at this point there can be no
      // active tokens. However it is possible that data was pushed to the
      // channel without being pulled. Run destructors for this data.
      close(); // ensure write_closed_at exists
      size_t woff = write_closed_at.load(std::memory_order_relaxed);
      size_t idx = read_offset.load(std::memory_order_relaxed);
      data_block* block = head_block.load(std::memory_order_acquire);
      while (circular_less_than(idx, woff)) {
        block = find_block(block, idx);
        element* elem = &block->values[idx & BlockSizeMask];
        if (elem->is_data_waiting()) {
          elem->data.destroy();
        }
        ++idx;
      }
    }
    {
      data_block* block = head_block.load(std::memory_order_acquire);
      while (block != nullptr) {
        data_block* next = block->next.load(std::memory_order_acquire);
        delete block;
        block = next;
      }
    }
    {
      hazard_ptr* start = hazard_ptr_list.load(std::memory_order_relaxed);
      hazard_ptr* curr = start;
      while (true) {
        assert(!curr->owned);
        hazard_ptr* next = curr->next.load(std::memory_order_relaxed);
        delete curr;
        if (next == start) {
          break;
        }
        curr = next;
      }
    }
  }

  channel(const channel&) = delete;
  channel& operator=(const channel&) = delete;
  channel(channel&&) = delete;
  channel& operator=(channel&&) = delete;
};

template <typename ContinuationPolicy, typename T, typename Config> class chan_tok_impl {
  using chan_t = channel<ContinuationPolicy, T, Config>;
  using hazard_ptr = coro_util::detail::hazard_ptr;
  std::shared_ptr<chan_t> chan;
  hazard_ptr* haz_ptr;
  CORO_UTIL_CHANNEL_NO_CONCURRENT_ACCESS_LOCK

  friend chan_tok_impl detail::make_channel<ContinuationPolicy, T, Config>() noexcept;

  chan_tok_impl(std::shared_ptr<chan_t>&& Chan) noexcept
      : chan{std::move(Chan)}, haz_ptr{nullptr} {}

  hazard_ptr* get_hazard_ptr() noexcept {
    if (haz_ptr == nullptr) [[unlikely]] {
      haz_ptr = chan->get_hazard_ptr();
    }
    return haz_ptr;
  }

  void free_hazard_ptr() noexcept {
    if (haz_ptr != nullptr) [[likely]] {
      haz_ptr->release_ownership();
      haz_ptr = nullptr;
    }
  }

public:
  /// If the channel is open, this will always return true, indicating that an
  /// object of type T was enqueued by in-place construction in the channel
  /// using the provided constructor arguments.
  ///
  /// If the channel is closed, this will return false, and the object will not
  /// be constructed.
  ///
  /// Will not suspend or block.
  template <typename... Args> bool post(Args&&... ConstructArgs) noexcept {
    // Implementing handling for throwing construction is not possible with the
    // current design. This assert will also fire if no matching constructor can
    // be found for the provided arguments.
    static_assert(std::is_nothrow_constructible_v<T, Args&&...>);
    CORO_UTIL_CHANNEL_ASSERT_NO_CONCURRENT_ACCESS();
    hazard_ptr* haz = get_hazard_ptr();
    return chan->post(haz, std::forward<Args>(ConstructArgs)...);
  }

  /// Await to dequeue. Returns a `chan_zc_scope` which provides a scoped
  /// zero-copy reference to a value in the channel storage. When the scope is
  /// destroyed, the referenced value will be destroyed and the channel slot
  /// freed for reuse. Use `value()`, `operator*()`, or `operator->()` to access
  /// the underlying value.
  ///
  /// The returned scope's `has_value()` / `operator bool()` returns true if a
  /// value was dequeued. If the channel is closed, this will continue to return
  /// values until the channel has been fully drained, after which it will resume
  /// with an empty scope (`has_value()` returns false).
  ///
  /// May suspend until a value is available, or the channel is closed.
  ///
  /// WARNING: The `chan_zc_scope` uses the same hazard pointer as this
  /// `chan_tok_impl` ! For correct operation, you MUST release or destroy the
  /// returned `chan_zc_scope` before calling another member function on this
  /// `chan_tok_impl`, and before this `chan_tok_impl` goes out of scope! The
  /// safest way to accomplish this is to tie its scope to the loop:
  /// `while (auto data = co_await chan.pull()) { process(data.value()); }`
  [[nodiscard(
    "You must co_await pull(). To poll from a non-coroutine function, use "
    "try_pull()."
  )]] chan_t::aw_pull
  pull() noexcept {
    CORO_UTIL_CHANNEL_ASSERT_NO_CONCURRENT_ACCESS();
    hazard_ptr* haz = get_hazard_ptr();
    return typename chan_t::aw_pull(*chan, haz);
  }

  /// Attempts to immediately dequeue, returning a `chan_try_pull_zc_scope`
  /// which provides a scoped zero-copy reference to a value in the channel
  /// storage. When the scope is destroyed, the referenced value will be
  /// destroyed and the channel slot freed for reuse. Use `value()`,
  /// `operator*()`, or `operator->()` to access the underlying value.
  ///
  /// The returned scope's `status()` returns:
  ///   - qu_err::OK     - a value was dequeued
  ///   - qu_err::EMPTY  - no value is currently available
  ///   - qu_err::CLOSED - the channel has been closed and drained
  ///
  /// The returned scope's `has_value()` / `operator bool()` returns true if a
  /// value was dequeued, or false if the channel was empty or closed.
  ///
  /// WARNING: The `chan_try_pull_zc_scope` uses the same hazard pointer as this
  /// `chan_tok_impl` ! For correct operation, you MUST release or destroy the
  /// returned scope before calling another member function on this
  /// `chan_tok_impl`, and before this `chan_tok_impl` goes out of scope! The
  /// safest way to accomplish this is to tie its scope to the loop:
  /// `while (auto data = chan.try_pull()) { process(data.value()); }`
  chan_try_pull_zc_scope<T> try_pull() noexcept {
    CORO_UTIL_CHANNEL_ASSERT_NO_CONCURRENT_ACCESS();
    hazard_ptr* haz = get_hazard_ptr();
    return chan->try_pull(haz);
  }

  /// If the channel is open, this will always return true, indicating that
  /// Count elements, starting from the Begin iterator, were enqueued.
  ///
  /// If the channel is closed, this will return false, and no items
  /// will be enqueued.
  ///
  /// Each item is moved (not copied) from the iterator into the channel.
  ///
  /// The closed check is performed first, then space is pre-allocated, then all
  /// Count items are moved into the channel. Thus, there cannot be a partial
  /// success - either all or none of the items will be moved.
  ///
  /// Will not suspend or block.
  template <typename TIter> bool post_bulk(TIter&& Begin, size_t Count) {
    // Implementing handling for throwing construction is not possible with the
    // current design. This assert will also fire if no matching constructor can
    // be found for the iterator's dereferenced value.
    static_assert(std::is_nothrow_constructible_v<T, decltype(std::move(*Begin))>);
    hazard_ptr* haz = get_hazard_ptr();
    return chan->post_bulk(haz, static_cast<TIter&&>(Begin), Count);
  }

  /// Calculates the number of elements via `size_t Count = End - Begin;`
  ///
  /// If the channel is open, this will always return true, indicating that
  /// Count elements, starting from the Begin iterator, were enqueued.
  ///
  /// If the channel is closed, this will return false, and no items
  /// will be enqueued.
  ///
  /// Each item is moved (not copied) from the iterator into the channel.
  ///
  /// The closed check is performed first, then space is pre-allocated, then all
  /// Count items are moved into the channel. Thus, there cannot be a partial
  /// success - either all or none of the items will be moved.
  ///
  /// Will not suspend or block.
  template <typename TIter> bool post_bulk(TIter&& Begin, TIter&& End) {
    // Implementing handling for throwing construction is not possible with the
    // current design. This assert will also fire if no matching constructor can
    // be found for the iterator's dereferenced value.
    static_assert(std::is_nothrow_constructible_v<T, decltype(std::move(*Begin))>);
    hazard_ptr* haz = get_hazard_ptr();
    return chan->post_bulk(
      haz, static_cast<TIter&&>(Begin), static_cast<size_t>(End - Begin)
    );
  }

  /// Calculates the number of elements via
  /// `size_t Count = Range.end() - Range.begin();`
  ///
  /// If the channel is open, this will always return true, indicating that
  /// Count elements from the beginning of the range were enqueued.
  ///
  /// If the channel is closed, this will return false, and no items
  /// will be enqueued.
  ///
  /// Each item is moved (not copied) from the iterator into the channel.
  ///
  /// The closed check is performed first, then space is pre-allocated, then all
  /// Count items are moved into the channel. Thus, there cannot be a partial
  /// success - either all or none of the items will be moved.
  ///
  /// Will not suspend or block.
  template <typename TRange> bool post_bulk(TRange&& Range) {
    // Implementing handling for throwing construction is not possible with the
    // current design. This assert will also fire if no matching constructor can
    // be found for the iterator's dereferenced value.
    static_assert(std::is_nothrow_constructible_v<
                  T, decltype(std::move(*static_cast<TRange&&>(Range).begin()))>);
    hazard_ptr* haz = get_hazard_ptr();
    auto begin = static_cast<TRange&&>(Range).begin();
    auto end = static_cast<TRange&&>(Range).end();
    return chan->post_bulk(haz, begin, static_cast<size_t>(end - begin));
  }

  /// All future calls to `post()` will immediately return false.
  /// Calls to `pull()` will continue to read data until all messages have been
  /// consumed, at which point all subsequent calls to `pull()` will immediately
  /// return an empty scope. If the queue was already empty, any waiting
  /// consumers will be awoken immediately and return an empty scope.
  ///
  /// This function is idempotent and thread-safe. It is not lock-free. It may
  /// contend the lock against other `close()` calls and block reclamation.
  void close() noexcept { chan->close(); }

  /// If true, spent blocks will be cleared and moved to the tail of the queue.
  /// If false, spent blocks will be deleted.
  /// Default: true
  chan_tok_impl& set_reuse_blocks(bool Reuse) noexcept {
    chan->ReuseBlocks.store(Reuse, std::memory_order_relaxed);
    return *this;
  }

  /// Copy Constructor: The new chan_tok_impl will have its own hazard pointer so
  /// that it can be used concurrently with the other token.
  ///
  /// If the other token is from a different channel, this token will now point
  /// to that channel.
  chan_tok_impl(const chan_tok_impl& Other) noexcept
      : chan(Other.chan), haz_ptr{nullptr} {}

  /// Copy Assignment: If the other token is from a different channel, this
  /// token will now point to that channel.
  chan_tok_impl& operator=(const chan_tok_impl& Other) noexcept {
    if (chan != Other.chan) {
      free_hazard_ptr();
      chan = Other.chan;
    }
    return *this;
  }

  /// Identical to the token copy constructor, but makes
  /// the intent more explicit - that a new token is being created which will
  /// independently own a reference count and hazard pointer to the underlying
  /// channel.
  chan_tok_impl new_token() noexcept { return chan_tok_impl(*this); }

  /// Move Constructor: The moved-from token will become empty; it will release
  /// its channel pointer, and its hazard pointer.
  chan_tok_impl(chan_tok_impl&& Other) noexcept
      : chan(std::move(Other.chan)), haz_ptr{Other.haz_ptr} {
    Other.haz_ptr = nullptr;
  }

  /// Move Assignment: The moved-from token will become empty; it will release
  /// its channel pointer, and its hazard pointer.
  ///
  /// If the other token is from a different channel, this token will now point
  /// to that channel.
  chan_tok_impl& operator=(chan_tok_impl&& Other) noexcept {
    if (chan != Other.chan) {
      free_hazard_ptr();
      haz_ptr = Other.haz_ptr;
      Other.haz_ptr = nullptr;
    } else {
      if (haz_ptr != nullptr) {
        // It's more efficient to keep our own hazptr
        Other.free_hazard_ptr();
      } else {
        haz_ptr = Other.haz_ptr;
        Other.haz_ptr = nullptr;
      }
    }
    chan = std::move(Other.chan);
    return *this;
  }

  /// Releases the token's hazard pointer and decrements the channel's shared
  /// reference count. When the last token for a channel is destroyed, the
  /// channel will also be destroyed. If the channel was not drained and any
  /// data remains in the channel, the destructor will also be called for each
  /// remaining data element.
  ~chan_tok_impl() { free_hazard_ptr(); }
};

namespace detail {
template <typename ContinuationPolicy, typename T, typename Config>
inline chan_tok_impl<ContinuationPolicy, T, Config> make_channel() noexcept {
  auto chan = new channel<ContinuationPolicy, T, Config>();
  return chan_tok_impl<ContinuationPolicy, T, Config>{
    std::shared_ptr<channel<ContinuationPolicy, T, Config>>(chan)
  };
}
} // namespace detail

} // namespace coro_util
