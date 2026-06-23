// Copyright (c) 2026 Logan McDougall
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits> // IWYU pragma: keep

#if defined(__x86_64__) || defined(_M_AMD64) || defined(i386) || defined(__i386__) ||    \
  defined(__i386) || defined(_M_IX86)
#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif
#endif

#if defined(_MSC_VER)

#ifdef __has_cpp_attribute

#if __has_cpp_attribute(msvc::forceinline)
#define CORO_UTIL_FORCE_INLINE [[msvc::forceinline]]
#else
#define CORO_UTIL_FORCE_INLINE
#endif

#if __has_cpp_attribute(msvc::no_unique_address)
#define CORO_UTIL_NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#else
#define CORO_UTIL_NO_UNIQUE_ADDRESS
#endif

#else // not __has_cpp_attribute
#define CORO_UTIL_FORCE_INLINE [[msvc::forceinline]]
#define CORO_UTIL_NO_UNIQUE_ADDRESS [[msvc::no_unique_address]]
#endif
#else // not _MSC_VER
#define CORO_UTIL_FORCE_INLINE __attribute__((always_inline))
#define CORO_UTIL_NO_UNIQUE_ADDRESS [[no_unique_address]]
#endif

#if defined(__clang__)
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_BEGIN
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_END
#elif defined(__GNUC__)
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_BEGIN                                 \
  _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wpessimizing-move\"")
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_END _Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_BEGIN
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_END
#else
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_BEGIN
#define CORO_UTIL_DISABLE_WARNING_PESSIMIZING_MOVE_END
#endif

// clang-format tries to collapse the pragmas into one line...
// clang-format off

// We like to pack pointers and flags into single words for performance.
// This depends on the expectation that nullptr == 0.
#if defined(__clang__)
_Pragma("clang diagnostic push")                                               
_Pragma("clang diagnostic ignored \"-Wzero-as-null-pointer-constant\"")
  static_assert(nullptr == 0, "nullptr is not 0 on this platform");
_Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
_Pragma("GCC diagnostic push")                                               
_Pragma("GCC diagnostic ignored \"-Wzero-as-null-pointer-constant\"")
  static_assert(nullptr == 0, "nullptr is not 0 on this platform");
_Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
#endif
// clang-format on

#if defined(__x86_64__) || defined(_M_AMD64) || defined(i386) || defined(__i386__) ||    \
  defined(__i386) || defined(_M_IX86)
  static inline void CORO_UTIL_CPU_PAUSE() noexcept {
  _mm_pause();
}
#elif defined(__arm__) || defined(_M_ARM) || defined(_M_ARM64) ||                        \
  defined(__aarch64__) || defined(__ARM_ACLE)
  static inline void CORO_UTIL_CPU_PAUSE() noexcept {
  asm volatile("yield");
}
#elif defined(__loongarch__) && defined(__LP64__)
static inline void CORO_UTIL_CPU_PAUSE() noexcept {
  for (int i = 0; i < 32; i++) {
    asm volatile("ibar 0");
  }
}
#elif defined(__riscv)
static inline void CORO_UTIL_CPU_PAUSE() noexcept {
#if defined(__riscv_zihintpause)
  asm volatile("pause" ::: "memory");
#else
  asm volatile("nop" ::: "memory");
#endif
}
#else
static inline void CORO_UTIL_CPU_PAUSE() noexcept {
  std::atomic_signal_fence(std::memory_order_seq_cst);
}
#endif

#if SIZE_MAX == 0xFFFFFFFFu
#define CORO_UTIL_PLATFORM_BITS 32
#else
#define CORO_UTIL_PLATFORM_BITS 64
#endif

// Apple M-series have 128-byte cache lines. This is not properly represented
// in `std::hardware_destructive_interference_size` on current Apple Clang.
#if defined(__aarch64__) && defined(__APPLE__)
inline constexpr size_t CORO_UTIL_CACHE_LINE_SIZE = 128;
#else
// GCC warns if we use std::hardware_destructive_interference_size here, so just
// use 64 instead. This is correct for the foreseeable future.
inline constexpr size_t CORO_UTIL_CACHE_LINE_SIZE = 64;
#endif

static_assert(std::atomic<void*>::is_always_lock_free);
static_assert(std::atomic<uintptr_t>::is_always_lock_free);
static_assert(std::atomic<size_t>::is_always_lock_free);

namespace coro_util {
namespace impl {
// Shared uninitialized storage type used by all queues.
// Lifecycle of the embedded object is managed by the queue.
template <typename T> struct qu_storage {
  union alignas(alignof(T)) {
    T value;
  };
#ifndef NDEBUG
  bool exists = false;
#endif

  qu_storage() noexcept {}

  template <typename... ConstructArgs> void emplace(ConstructArgs&&... Args) noexcept {
#ifndef NDEBUG
    assert(!exists);
    exists = true;
#endif
    ::new (static_cast<void*>(&value)) T(static_cast<ConstructArgs&&>(Args)...);
  }

  void destroy() noexcept {
#ifndef NDEBUG
    assert(exists);
    exists = false;
#endif
    value.~T();
  }

  // Precondition: Other.value must exist
  qu_storage(qu_storage&& Other) noexcept {
    emplace(static_cast<T&&>(Other.value));
    Other.destroy();
  }
  qu_storage& operator=(qu_storage&& Other) noexcept {
    emplace(static_cast<T&&>(Other.value));
    Other.destroy();
    return *this;
  }

  // If data was present, the caller is responsible for destroying it.
#ifndef NDEBUG
  ~qu_storage() { assert(!exists); }
#else
  ~qu_storage()
    requires(std::is_trivially_destructible_v<T>)
  = default;
  ~qu_storage()
    requires(!std::is_trivially_destructible_v<T>)
  {}
#endif

  qu_storage(const qu_storage&) = delete;
  qu_storage& operator=(const qu_storage&) = delete;
};
} // namespace impl

/// Status code returned by try_pull().status()
enum class qu_err { OK, EMPTY, CLOSED };

} // namespace coro_util
