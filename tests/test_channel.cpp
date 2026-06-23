#include "coro_util/tmc/channel.hpp"
#include "test_common.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <gtest/gtest.h>
#include <ranges>
#include <thread>
#include <utility>

#define CATEGORY test_channel

namespace {

class CATEGORY : public testing::Test {
protected:
  static void SetUpTestSuite() { tmc::cpu_executor().init(); }

  static void TearDownTestSuite() { tmc::cpu_executor().teardown(); }

  static tmc::ex_cpu& ex() { return tmc::cpu_executor(); }
};

template <bool Padding> struct chan_config : coro_util::chan_default_config {
  // Use a small block size to ensure that alloc / reclaim is triggered.
  static inline constexpr size_t BlockSize = 2;
  static inline constexpr bool ElementPadding = Padding;
};

// multiple tests in one to leverage the configuration options in one place
template <bool ElementPadding, typename Executor>
void do_chan_test(Executor& Exec, bool ReuseBlocks, bool TryPull) {
  test_async_main(Exec, [](bool Reuse, bool Try) -> tmc::task<void> {
    {
      // general test - single post
      static constexpr size_t NITEMS = 1000;
      struct result {
        size_t count;
        size_t sum;
      };

      auto chan =
        coro_util::make_channel<size_t, chan_config<ElementPadding>>().set_reuse_blocks(
          Reuse
        );

      auto results = co_await tmc::spawn_tuple(
        [](auto Chan) -> tmc::task<size_t> {
          size_t i = 0;
          for (; i < NITEMS; ++i) {
            bool ok = Chan.post(i);
            EXPECT_EQ(true, ok);
          }
          Chan.close();
          co_return i;
        }(chan),
        [](auto Chan, bool DoTry) -> tmc::task<result> {
          size_t count = 0;
          size_t sum = 0;
          while (true) {
            if (!DoTry) {
              auto data = co_await Chan.pull();
              if (data.has_value()) {
                ++count;
                sum += data.value();
              } else {
                co_return result{count, sum};
              }
            } else {
              auto data = Chan.try_pull();
              switch (data.status()) {
              case coro_util::qu_err::OK:
                ++count;
                sum += data.value();
                break;
              case coro_util::qu_err::EMPTY:
                co_await tmc::reschedule();
                break;
              case coro_util::qu_err::CLOSED:
                co_return result{count, sum};
              }
            }
          }
        }(chan, Try)
      );
      auto& prod = std::get<0>(results);
      auto& cons = std::get<1>(results);
      EXPECT_EQ(NITEMS, prod);
      EXPECT_EQ(NITEMS, cons.count);
      size_t expectedSum = 0;
      for (size_t i = 0; i < NITEMS; ++i) {
        expectedSum += i;
      }
      EXPECT_EQ(expectedSum, cons.sum);
    }
    {
      // general test - post_bulk
      static constexpr size_t NITEMS = 1000;
      struct result {
        size_t count;
        size_t sum;
      };

      auto chan =
        coro_util::make_channel<size_t, chan_config<ElementPadding>>().set_reuse_blocks(
          Reuse
        );

      auto results = co_await tmc::spawn_tuple(
        [](auto Chan) -> tmc::task<size_t> {
          size_t i = 0;
          for (; i < NITEMS; i += (NITEMS / 10)) {
            size_t j = i + (NITEMS / 10);
            if (j > NITEMS) {
              j = NITEMS;
            }
            bool ok = Chan.post_bulk(std::ranges::views::iota(i, j));
            EXPECT_EQ(true, ok);
          }
          Chan.close();
          co_return i;
        }(chan),
        [](auto Chan, bool DoTry) -> tmc::task<result> {
          size_t count = 0;
          size_t sum = 0;
          while (true) {
            if (!DoTry) {
              auto data = co_await Chan.pull();
              if (data.has_value()) {
                ++count;
                sum += data.value();
              } else {
                co_return result{count, sum};
              }
            } else {
              auto data = Chan.try_pull();
              switch (data.status()) {
              case coro_util::qu_err::OK:
                ++count;
                sum += data.value();
                break;
              case coro_util::qu_err::EMPTY:
                co_await tmc::reschedule();
                break;
              case coro_util::qu_err::CLOSED:
                co_return result{count, sum};
              }
            }
          }
        }(chan, Try)
      );
      auto& prod = std::get<0>(results);
      auto& cons = std::get<1>(results);
      EXPECT_EQ(NITEMS, prod);
      EXPECT_EQ(NITEMS, cons.count);
      size_t expectedSum = 0;
      for (size_t i = 0; i < NITEMS; ++i) {
        expectedSum += i;
      }
      EXPECT_EQ(expectedSum, cons.sum);
    }
    {
      // destroy chan with data remaining inside
      std::atomic<size_t> count{0};
      {
        auto chan =
          coro_util::make_channel<destructor_counter, chan_config<ElementPadding>>()
            .set_reuse_blocks(Reuse);
        for (size_t i = 0; i < 12; ++i) {
          chan.post(destructor_counter{&count});
        }

        for (size_t i = 0; i < 7; ++i) {
          co_await chan.pull();
        }

        EXPECT_EQ(count.load(), 7);
      }
      // Now chan goes out of scope; remaining data's destructors are called
      EXPECT_EQ(count.load(), 12);
    }
    {
      // producer post / post_bulk after chan closed
      auto chan =
        coro_util::make_channel<size_t, chan_config<ElementPadding>>().set_reuse_blocks(
          Reuse
        );
      chan.close();
      auto p = chan.post(5u);
      EXPECT_FALSE(p);
      std::vector<size_t> vs{0, 1, 2, 3, 4};
      auto p1 = chan.post_bulk(vs.begin(), 5);
      EXPECT_FALSE(p1);
      auto p2 = chan.post_bulk(vs.begin(), vs.end());
      EXPECT_FALSE(p2);
      auto p3 = chan.post_bulk(std::ranges::views::iota(0u, 5u));
      EXPECT_FALSE(p3);
    }
    {
      // close while there is a waiting consumer
      auto chan =
        coro_util::make_channel<size_t, chan_config<ElementPadding>>().set_reuse_blocks(
          Reuse
        );
      std::array<tmc::task<void>, 5> cons;
      for (size_t i = 0; i < 5; ++i) {
        cons[i] = [](auto Chan) -> tmc::task<void> {
          auto v = co_await Chan.pull();
          EXPECT_FALSE(v.has_value());
        }(chan);
      }
      auto t = tmc::spawn_many<5>(cons.data()).fork();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      chan.close();
      co_await std::move(t);
    }
  }(ReuseBlocks, TryPull));
}

TEST_F(CATEGORY, config_sweep) {
  do_chan_test<true>(ex(), false, false);
  do_chan_test<true>(ex(), true, false);
  do_chan_test<false>(ex(), false, false);
  do_chan_test<false>(ex(), true, false);
}

TEST_F(CATEGORY, config_sweep_try_pull) {
  do_chan_test<true>(ex(), false, true);
  do_chan_test<true>(ex(), true, true);
  do_chan_test<false>(ex(), false, true);
  do_chan_test<false>(ex(), true, true);
}

// Running 1 consumer and 1 producer at the same time on a single thread
// To ensure there are no deadlocks
TEST_F(CATEGORY, post_single_threaded) {
  tmc::ex_cpu ex;
  ex.set_thread_count(1).init();
  test_async_main(ex, []() -> tmc::task<void> {
    static constexpr size_t NITEMS = 100000;
    auto chan = coro_util::make_channel<size_t>();
    struct result {
      size_t count;
      size_t sum;
    };

    auto results = co_await tmc::spawn_tuple(
      [](auto Chan) -> tmc::task<size_t> {
        size_t i = 0;
        for (; i < NITEMS; ++i) {
          bool ok;
          if ((i & 0x2) == 0) {
            // 2 posts
            ok = Chan.post(i);
          } else {
            // then 2 post_bulks
            ok = Chan.post_bulk(std::ranges::views::iota(i, i + 1));
          }
          EXPECT_EQ(true, ok);
        }
        Chan.close();
        co_return i;
      }(chan),
      [](auto Chan) -> tmc::task<result> {
        size_t count = 0;
        size_t sum = 0;
        while (auto v = co_await Chan.pull()) {
          sum += v.value();
          ++count;
        }
        co_return result{count, sum};
      }(chan)
    );
    auto& prod = std::get<0>(results);
    auto& cons = std::get<1>(results);
    EXPECT_EQ(NITEMS, prod);
    EXPECT_EQ(NITEMS, cons.count);
    size_t expectedSum = 0;
    for (size_t i = 0; i < NITEMS; ++i) {
      expectedSum += i;
    }
    EXPECT_EQ(expectedSum, cons.sum);
  }());
}

// Test post_bulk of 0 items
TEST_F(CATEGORY, post_bulk_none) {
  tmc::ex_cpu ex;
  ex.set_thread_count(1).init();
  test_async_main(ex, []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    size_t i = 0;
    bool ok;
    for (; i < 4; ++i) {
      ok = chan.post_bulk(std::ranges::views::iota(i, i));
      EXPECT_EQ(true, ok);
      ok = chan.post_bulk(std::ranges::views::iota(i, i));
      EXPECT_EQ(true, ok);
      ok = chan.post(i);
      EXPECT_EQ(true, ok);
    }
    for (; i < 8; ++i) {
      ok = chan.post_bulk(std::ranges::views::iota(i, i));
      EXPECT_EQ(true, ok);
      ok = chan.post_bulk(std::ranges::views::iota(i, i));
      EXPECT_EQ(true, ok);
      ok = chan.post_bulk(std::ranges::views::iota(i, i + 1));
      EXPECT_EQ(true, ok);
    }
    size_t count = 0;
    size_t sum = 0;
    for (size_t j = 0; j < i; ++j) {
      auto v = co_await chan.pull();
      sum += v.value();
      ++count;
    }
    chan.close();
    EXPECT_EQ(28, sum);
    EXPECT_EQ(8, count);

    auto v = co_await chan.pull();
    EXPECT_FALSE(v.has_value());

    ok = chan.post_bulk(std::ranges::views::iota(1u, 1u));
    EXPECT_EQ(false, ok);
    ok = chan.post_bulk(std::ranges::views::iota(1u, 2u));
    EXPECT_EQ(false, ok);
    ok = chan.post(5u);
    EXPECT_EQ(false, ok);
  }());
}

// Demonstrate passing a move-only type through the channel.
struct move_only_type {
  int value;

  move_only_type(int input) : value(input) {}
  move_only_type& operator=(move_only_type&&) = default;
  move_only_type(move_only_type&&) = default;
  ~move_only_type() = default;

  // No default or copy constructor
  move_only_type() = delete;
  move_only_type(const move_only_type&) = delete;
  move_only_type& operator=(const move_only_type&) = delete;
};

TEST_F(CATEGORY, move_only_type) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<move_only_type, chan_config<true>>();

    chan.post(move_only_type(0));

    auto mt = move_only_type(1);
    chan.post(std::move(mt));

    chan.post(move_only_type(2));

    auto mt1 = move_only_type(3);
    chan.post(std::move(mt1));

    // Each pull() scope must be released before the next channel operation,
    // since it shares the token's hazard pointer.
    {
      auto v0 = co_await chan.pull();
      EXPECT_TRUE(v0.has_value());
      EXPECT_EQ(v0.value().value, 0);
    }

    {
      auto v1 = chan.try_pull();
      EXPECT_EQ(v1.status(), coro_util::qu_err::OK);
      EXPECT_EQ(v1.value().value, 1);
    }

    {
      auto v2 = co_await chan.pull();
      EXPECT_TRUE(v2.has_value());
      EXPECT_EQ(v2.value().value, 2);
    }

    {
      auto v3 = chan.try_pull();
      EXPECT_EQ(v3.status(), coro_util::qu_err::OK);
      EXPECT_EQ(v3.value().value, 3);
    }
  }());
}

struct move_counter {
  int value;
  std::atomic<size_t>* count;

  move_counter(int v, std::atomic<size_t>* c) noexcept : value(v), count(c) {}

  move_counter(move_counter&& Other) noexcept : value(Other.value), count(Other.count) {
    Other.count = nullptr;
    if (count != nullptr) {
      ++(*count);
    }
  }

  move_counter& operator=(move_counter&& Other) noexcept {
    value = Other.value;
    count = Other.count;
    Other.count = nullptr;
    if (count != nullptr) {
      ++(*count);
    }
    return *this;
  }

  ~move_counter() = default;

  move_counter() = delete;
  move_counter(const move_counter&) = delete;
  move_counter& operator=(const move_counter&) = delete;
};

// Both pull() and try_pull() are zero-copy: the value is accessed in place in
// the channel storage and is never moved out, so no move constructor is called.
TEST_F(CATEGORY, move_count_post_pull) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<move_counter, chan_config<true>>();
    std::atomic<size_t> moves{0};

    chan.post(42, &moves);
    auto v = co_await chan.pull();

    EXPECT_TRUE(v.has_value());
    EXPECT_EQ(v.value().value, 42);
    EXPECT_EQ(moves.load(), 0);
  }());
}

TEST_F(CATEGORY, move_count_post_try_pull) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<move_counter, chan_config<true>>();
    std::atomic<size_t> moves{0};

    chan.post(42, &moves);
    auto v = chan.try_pull();

    EXPECT_EQ(v.status(), coro_util::qu_err::OK);
    EXPECT_EQ(v.value().value, 42);
    EXPECT_EQ(moves.load(), 0);
    co_return;
  }());
}

// Test zero-copy pull functionality
TEST_F(CATEGORY, pull_zc) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<move_counter, chan_config<true>>();
    std::atomic<size_t> moves{0};

    chan.post(42, &moves);

    {
      auto scope = co_await chan.pull();
      EXPECT_TRUE(scope.has_value());
      // Test chan_zc_scope::value()
      EXPECT_EQ(scope.value().value, 42);
      // Test chan_zc_scope::operator*
      EXPECT_EQ((*scope).value, 42);
      // Test chan_zc_scope::operator->
      EXPECT_EQ(scope->value, 42);
      // Should be zero moves - data accessed directly in channel storage
      EXPECT_EQ(moves.load(), 0);
    }
    // Scope destroyed, slot released

    // Verify channel is empty now
    chan.close();
    auto v = co_await chan.pull();
    EXPECT_FALSE(v.has_value());
  }());
}

// This struct can't be moved and tracks the number of times it was destroyed.
// Destructor count should be 1 (obviously) unless there is a double-destroy.
struct immovable_destructor_counter {
  size_t value;
  std::atomic<size_t>* count;

  immovable_destructor_counter(size_t v, std::atomic<size_t>* c) noexcept
      : value(v), count(c) {}

  ~immovable_destructor_counter() { ++(*count); }

  immovable_destructor_counter() = delete;

  immovable_destructor_counter(const immovable_destructor_counter&) = delete;
  immovable_destructor_counter& operator=(const immovable_destructor_counter&) = delete;

  immovable_destructor_counter(immovable_destructor_counter&& Other) = delete;
  immovable_destructor_counter& operator=(immovable_destructor_counter&& Other) = delete;
};

TEST_F(CATEGORY, pull_zc_immovable) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan =
      coro_util::make_channel<immovable_destructor_counter, chan_config<true>>();
    std::array<std::atomic<size_t>, 100> destroys{};

    for (size_t i = 0; i < destroys.size(); ++i) {
      chan.post(i, &destroys[i]);
    }
    chan.close();

    size_t taskCount = 0;
    while (auto data = co_await chan.pull()) {
      EXPECT_EQ(data->value, taskCount);
      ++taskCount;
    }
    EXPECT_EQ(taskCount, destroys.size());

    for (size_t i = 0; i < destroys.size(); ++i) {
      EXPECT_EQ(destroys[i].load(), 1u);
    }

    // Verify channel is empty now
    auto v = co_await chan.pull();
    EXPECT_FALSE(v.has_value());
  }());
}

// Test zero-copy pull with channel close
TEST_F(CATEGORY, pull_zc_closed) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    chan.post(1u);
    chan.post(2u);
    chan.close();

    {
      auto s1 = co_await chan.pull();
      EXPECT_TRUE(s1.has_value());
      EXPECT_EQ(s1.value(), 1);
    }
    {
      auto s2 = co_await chan.pull();
      EXPECT_TRUE(s2.has_value());
      EXPECT_EQ(s2.value(), 2);
    }
    auto s3 = co_await chan.pull();
    EXPECT_FALSE(s3.has_value());
  }());
}

// Test close from a non-executor thread while a consumer drains the channel.
TEST_F(CATEGORY, close_from_external_thread) {
  auto chan = coro_util::make_channel<size_t, chan_config<true>>();

  std::thread producer([&chan]() {
    for (size_t i = 0; i < 10; ++i) {
      chan.post(i);
    }
    chan.close();
  });

  test_async_main(ex(), [](auto Chan) -> tmc::task<void> {
    size_t count = 0;
    while (auto v = co_await Chan.pull()) {
      ++count;
    }
    EXPECT_EQ(count, 10);
  }(chan));

  producer.join();
}

TEST_F(CATEGORY, close_idempotent) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    auto consumer = tmc::spawn([](auto Chan) -> tmc::task<size_t> {
                      size_t count = 0;
                      while (co_await Chan.pull()) {
                        ++count;
                      }
                      co_return count;
                    }(chan.new_token()))
                      .fork();

    EXPECT_TRUE(chan.post(1u));
    EXPECT_TRUE(chan.post(2u));

    chan.close();
    chan.close();

    chan.close();
    chan.close();

    EXPECT_EQ(co_await std::move(consumer), 2u);

    auto v = co_await chan.pull();
    EXPECT_FALSE(v.has_value());
  }());
}

TEST_F(CATEGORY, close_idempotent_external_thread) {
  auto chan = coro_util::make_channel<size_t, chan_config<true>>();
  auto closeToken = chan.new_token();

  EXPECT_TRUE(chan.post(1u));
  EXPECT_TRUE(chan.post(2u));
  EXPECT_TRUE(chan.post(3u));

  std::thread closer([closeToken]() mutable {
    closeToken.close();
    closeToken.close();
    closeToken.close();
    closeToken.close();
  });

  test_async_main(ex(), [consumer = chan.new_token()]() mutable -> tmc::task<void> {
    size_t count = 0;
    while (co_await consumer.pull()) {
      ++count;
    }
    EXPECT_EQ(count, 3u);
  }());

  closer.join();
}

// A post() that fails due to the channel being closed still consumes a
// write_offset slot. try_pull() must still report CLOSED (not EMPTY) on a
// fully-drained channel afterwards, or a polling consumer would spin forever.
TEST_F(CATEGORY, try_pull_closed_after_failed_post) {
  auto chan = coro_util::make_channel<size_t, chan_config<true>>();

  EXPECT_TRUE(chan.post(1u));
  {
    auto v = chan.try_pull();
    EXPECT_EQ(v.status(), coro_util::qu_err::OK);
    EXPECT_EQ(v.value(), 1u);
  }

  chan.close();
  EXPECT_FALSE(chan.post(2u));
  chan.close();

  {
    auto v = chan.try_pull();
    EXPECT_EQ(v.status(), coro_util::qu_err::CLOSED);
  }
  {
    auto v = chan.try_pull();
    EXPECT_EQ(v.status(), coro_util::qu_err::CLOSED);
  }
  EXPECT_FALSE(chan.post(3u));
  {
    auto v = chan.try_pull();
    EXPECT_EQ(v.status(), coro_util::qu_err::CLOSED);
  }
  {
    auto v = chan.try_pull();
    EXPECT_EQ(v.status(), coro_util::qu_err::CLOSED);
  }
}

TEST_F(CATEGORY, close_wakes_waiting_consumers) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    std::array<tmc::task<void>, 5> consumers;
    for (auto& consumer : consumers) {
      consumer = [](auto Chan) -> tmc::task<void> {
        auto v = co_await Chan.pull();
        EXPECT_FALSE(v.has_value());
      }(chan.new_token());
    }

    auto waiting = tmc::spawn_many<5>(consumers.data()).fork();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    chan.close();

    co_await std::move(waiting);
  }());
}

// Test new_token() explicit token creation
TEST_F(CATEGORY, new_token) {
  test_async_main(ex(), []() -> tmc::task<void> {
    static constexpr size_t NITEMS = 100;
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();

    auto results = co_await tmc::spawn_tuple(
      [](auto Chan) -> tmc::task<size_t> {
        for (size_t i = 0; i < NITEMS; ++i) {
          Chan.post(i);
        }
        Chan.close();
        co_return NITEMS;
      }(chan.new_token()),
      [](auto Chan) -> tmc::task<size_t> {
        size_t count = 0;
        while (co_await Chan.pull()) {
          ++count;
        }
        co_return count;
      }(chan.new_token())
    );
    EXPECT_EQ(std::get<0>(results), NITEMS);
    EXPECT_EQ(std::get<1>(results), NITEMS);
  }());
}

// Test token copy and move semantics
TEST_F(CATEGORY, token_copy_move) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan1 = coro_util::make_channel<size_t, chan_config<true>>();

    // Each pull() scope shares its token's hazard pointer, so it must be
    // released before the next operation on that token (here, before the token
    // is moved-from or reassigned).

    // Test copy constructor
    auto chan2 = chan1;
    chan1.post(1u);
    {
      auto v = co_await chan2.pull();
      EXPECT_TRUE(v.has_value());
      EXPECT_EQ(v.value(), 1);
    }

    // Test move constructor
    auto chan3 = std::move(chan2);
    chan1.post(2u);
    {
      auto v = co_await chan3.pull();
      EXPECT_TRUE(v.has_value());
      EXPECT_EQ(v.value(), 2);
    }

    // Test copy assignment
    auto chan4 = coro_util::make_channel<size_t, chan_config<true>>();
    chan4 = chan1;
    chan1.post(3u);
    {
      auto v = co_await chan4.pull();
      EXPECT_TRUE(v.has_value());
      EXPECT_EQ(v.value(), 3);
    }

    // Test move assignment
    auto chan5 = coro_util::make_channel<size_t, chan_config<true>>();
    chan5 = std::move(chan4);
    chan1.post(4u);
    {
      auto v = co_await chan5.pull();
      EXPECT_TRUE(v.has_value());
      EXPECT_EQ(v.value(), 4);
    }

    // Test move assignment between two tokens of the SAME channel, where the
    // destination already holds its own hazard pointer. The more-efficient path
    // keeps the destination's hazptr and releases the source's.
    {
      auto tokA = chan1.new_token();
      auto tokB = chan1.new_token();
      chan1.post(5u);
      {
        auto v = co_await tokA.pull(); // tokA acquires its own hazard pointer
        EXPECT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), 5);
      }
      chan1.post(6u);
      {
        auto v = co_await tokB.pull(); // tokB acquires its own hazard pointer
        EXPECT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), 6);
      }
      tokA = std::move(tokB); // same channel: tokA keeps its hazptr, frees tokB's
      chan1.post(7u);
      {
        auto v = co_await tokA.pull();
        EXPECT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), 7);
      }
    }

    // Test move assignment between two tokens of the SAME channel, where the
    // destination has not yet acquired a hazard pointer (a fresh token), so it
    // adopts the source's.
    {
      auto tokC = chan1.new_token(); // no operations yet: haz_ptr is null
      auto tokD = chan1.new_token();
      chan1.post(8u);
      {
        auto v = co_await tokD.pull(); // tokD acquires its own hazard pointer
        EXPECT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), 8);
      }
      tokC = std::move(tokD); // same channel: tokC adopts tokD's hazptr
      chan1.post(9u);
      {
        auto v = co_await tokC.pull();
        EXPECT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), 9);
      }
    }
  }());
}

// Test with multiple producers and consumers
TEST_F(CATEGORY, mpmc) {
  test_async_main(ex(), []() -> tmc::task<void> {
    static constexpr size_t NPRODUCERS = 4;
    static constexpr size_t NCONSUMERS = 4;
    static constexpr size_t ITEMS_PER_PRODUCER = 1000;

    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    std::atomic<size_t> totalProduced{0};
    std::atomic<size_t> totalConsumed{0};
    std::atomic<size_t> consumedSum{0};

    std::array<tmc::task<void>, NPRODUCERS> producers;
    for (size_t p = 0; p < NPRODUCERS; ++p) {
      producers[p] = [](
                       auto Chan, std::atomic<size_t>* Produced, size_t ProducerId
                     ) -> tmc::task<void> {
        for (size_t i = 0; i < ITEMS_PER_PRODUCER; ++i) {
          size_t val = ProducerId * ITEMS_PER_PRODUCER + i;
          Chan.post(val);
          Produced->fetch_add(1, std::memory_order_relaxed);
        }
        co_return;
      }(chan.new_token(), &totalProduced, p);
    }

    std::array<tmc::task<void>, NCONSUMERS> consumers;
    for (size_t c = 0; c < NCONSUMERS; ++c) {
      consumers[c] = [](
                       auto Chan, std::atomic<size_t>* Consumed, std::atomic<size_t>* Sum
                     ) -> tmc::task<void> {
        while (auto v = co_await Chan.pull()) {
          Consumed->fetch_add(1, std::memory_order_relaxed);
          Sum->fetch_add(v.value(), std::memory_order_relaxed);
        }
      }(chan.new_token(), &totalConsumed, &consumedSum);
    }

    auto prods = tmc::spawn_many<NPRODUCERS>(producers.data()).fork();
    auto cons = tmc::spawn_many<NCONSUMERS>(consumers.data()).fork();

    co_await std::move(prods);
    chan.close();
    co_await std::move(cons);

    EXPECT_EQ(totalProduced.load(), NPRODUCERS * ITEMS_PER_PRODUCER);
    EXPECT_EQ(totalConsumed.load(), NPRODUCERS * ITEMS_PER_PRODUCER);

    size_t expectedSum = 0;
    for (size_t p = 0; p < NPRODUCERS; ++p) {
      for (size_t i = 0; i < ITEMS_PER_PRODUCER; ++i) {
        expectedSum += p * ITEMS_PER_PRODUCER + i;
      }
    }
    EXPECT_EQ(consumedSum.load(), expectedSum);
  }());
}

// Test post_bulk with iterator + count overload
TEST_F(CATEGORY, post_bulk_iterator_count) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();

    std::vector<size_t> items{10, 20, 30, 40, 50};
    bool ok = chan.post_bulk(items.begin(), 3); // Only post first 3
    EXPECT_TRUE(ok);

    // Each pull() scope must be released before the next channel operation
    // (another pull() or the post_bulk() below), since it shares the token's
    // hazard pointer.
    {
      auto v1 = co_await chan.pull();
      EXPECT_TRUE(v1.has_value());
      EXPECT_EQ(v1.value(), 10);
    }
    {
      auto v2 = co_await chan.pull();
      EXPECT_TRUE(v2.has_value());
      EXPECT_EQ(v2.value(), 20);
    }
    {
      auto v3 = co_await chan.pull();
      EXPECT_TRUE(v3.has_value());
      EXPECT_EQ(v3.value(), 30);
    }

    // Post remaining items with begin/end overload
    ok = chan.post_bulk(items.begin() + 3, items.end());
    EXPECT_TRUE(ok);

    {
      auto v4 = co_await chan.pull();
      EXPECT_TRUE(v4.has_value());
      EXPECT_EQ(v4.value(), 40);
    }
    {
      auto v5 = co_await chan.pull();
      EXPECT_TRUE(v5.has_value());
      EXPECT_EQ(v5.value(), 50);
    }
  }());
}

// Test chan_zc_scope move semantics
TEST_F(CATEGORY, zc_scope_move) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    chan.post(42u);

    auto scope1 = co_await chan.pull();
    EXPECT_TRUE(scope1.has_value());

    // Move the scope
    auto scope2 = std::move(scope1);
    EXPECT_EQ(scope2.value(), 42);

    // Original scope1 was moved from and no longer holds a value
    EXPECT_FALSE(scope1.has_value());

    chan.close();
  }());
}

TEST_F(CATEGORY, close_empty_channel) {
  test_async_main(ex(), []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    chan.close();
    co_return;
  }());
}

TEST_F(CATEGORY, close_empty_channel_external_thread) {
  auto chan = coro_util::make_channel<size_t, chan_config<true>>();
  chan.close();
}

// Exercise the chan_try_pull_zc_scope accessors that the existing try_pull
// tests never touch: operator bool, has_value(), operator*, and operator->.
// (Those tests only call status() and value() on a try_pull result.)
TEST_F(CATEGORY, try_pull_scope_accessors) {
  test_async_main(ex(), []() -> tmc::task<void> {
    // Use size_t (already exercised elsewhere) to avoid instantiating a fresh
    // channel<T> whose reclaim paths this small test would leave uncovered.
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();

    // Default constructor: an empty scope.
    decltype(chan.try_pull()) empty{};
    EXPECT_FALSE(static_cast<bool>(empty));
    EXPECT_FALSE(empty.has_value());

    chan.post(7u);

    auto v = chan.try_pull();
    EXPECT_TRUE(static_cast<bool>(v)); // operator bool
    EXPECT_TRUE(v.has_value());        // has_value()
    EXPECT_EQ(*v, 7u);                 // operator*
    EXPECT_EQ(*v.operator->(), 7u);    // operator-> (size_t has no members)

    // Move constructor.
    auto moved = std::move(v);
    EXPECT_FALSE(v.has_value());
    EXPECT_TRUE(moved.has_value());
    EXPECT_EQ(moved.value(), 7u);
    co_return;
  }());
}

// Exhaustively exercise chan_try_pull_zc_scope::operator=(&&): over-nonempty,
// move-into-empty, and self-move. Separate channels are used because a try_pull
// scope shares its token's single hazard pointer, so at most one scope per
// channel may be live at a time.
TEST_F(CATEGORY, chan_try_pull_zc_scope_move_assign_branches) {
  test_async_main(ex(), []() -> tmc::task<void> {
    std::atomic<size_t> count1{0};
    std::atomic<size_t> count2{0};
    std::atomic<size_t> count3{0};
    {
      auto c1 = coro_util::make_channel<destructor_counter, chan_config<true>>();
      auto c2 = coro_util::make_channel<destructor_counter, chan_config<true>>();
      auto c3 = coro_util::make_channel<destructor_counter, chan_config<true>>();
      c1.post(destructor_counter{&count1});
      c2.post(destructor_counter{&count2});
      c3.post(destructor_counter{&count3});

      auto a = c1.try_pull();
      EXPECT_TRUE(a.has_value());

      // Over-nonempty: a holds c1; assigning c2 destroys c1's element first.
      a = c2.try_pull();
      EXPECT_EQ(1u, count1.load());
      EXPECT_EQ(0u, count2.load());

      // Move-into-empty: b adopts c2 from a (a empty); assigning c3 into the
      // empty a skips the release path.
      auto b = std::move(a);
      EXPECT_FALSE(a.has_value());
      a = c3.try_pull();
      EXPECT_TRUE(a.has_value());
      EXPECT_EQ(0u, count2.load()); // c2 still alive in b
      EXPECT_EQ(0u, count3.load());

      // Self-move: the `this != &Other` guard takes its false arm; value kept.
      auto& aref = a;
      a = std::move(aref);
      EXPECT_TRUE(a.has_value());
      EXPECT_EQ(0u, count3.load());
    } // ~b releases c2's element, ~a releases c3's element.
    EXPECT_EQ(1u, count1.load());
    EXPECT_EQ(1u, count2.load());
    EXPECT_EQ(1u, count3.load());
    co_return;
  }());
}

// Exhaustively exercise chan_zc_scope::operator=(&&) (returned by co_await
// pull()): over-nonempty, move-into-empty, and self-move.
TEST_F(CATEGORY, chan_zc_scope_move_assign_branches) {
  test_async_main(ex(), []() -> tmc::task<void> {
    std::atomic<size_t> count1{0};
    std::atomic<size_t> count2{0};
    std::atomic<size_t> count3{0};
    {
      auto c1 = coro_util::make_channel<destructor_counter, chan_config<true>>();
      auto c2 = coro_util::make_channel<destructor_counter, chan_config<true>>();
      auto c3 = coro_util::make_channel<destructor_counter, chan_config<true>>();
      c1.post(destructor_counter{&count1});
      c2.post(destructor_counter{&count2});
      c3.post(destructor_counter{&count3});

      auto a = co_await c1.pull();
      EXPECT_TRUE(a.has_value());

      // Over-nonempty.
      a = co_await c2.pull();
      EXPECT_EQ(1u, count1.load());
      EXPECT_EQ(0u, count2.load());

      // Move-into-empty.
      auto b = std::move(a);
      EXPECT_FALSE(a.has_value());
      a = co_await c3.pull();
      EXPECT_TRUE(a.has_value());
      EXPECT_EQ(0u, count2.load());
      EXPECT_EQ(0u, count3.load());

      // Self-move.
      auto& aref = a;
      a = std::move(aref);
      EXPECT_TRUE(a.has_value());
      EXPECT_EQ(0u, count3.load());
    }
    EXPECT_EQ(1u, count1.load());
    EXPECT_EQ(1u, count2.load());
    EXPECT_EQ(1u, count3.load());
    co_return;
  }());
}

// try_pull() must report CLOSED (not EMPTY) once read_offset has drained up to
// write_offset on a closed channel. This hits the "appears empty by index AND
// closed" branch in try_pull. The existing try_pull_closed_after_failed_post
// test reaches CLOSED via the other branch (failed posts after close leave
// write_offset ahead of read_offset, so the queue still appears non-empty).
TEST_F(CATEGORY, try_pull_closed_after_drain) {
  tmc::ex_cpu ex;
  ex.set_thread_count(1).init();
  test_async_main(ex, []() -> tmc::task<void> {
    auto chan = coro_util::make_channel<size_t, chan_config<true>>();
    EXPECT_TRUE(chan.post(1u));
    chan.close();

    // Drain the one real item, then pull past the close sentinel so that
    // read_offset catches up to write_offset.
    {
      auto v = co_await chan.pull();
      EXPECT_TRUE(v.has_value());
      EXPECT_EQ(v.value(), 1u);
    }
    {
      auto v2 = co_await chan.pull();
      EXPECT_FALSE(v2.has_value());
    }

    // read_offset == write_offset and the channel is closed: try_pull takes the
    // empty-by-index closed branch and returns CLOSED rather than EMPTY.
    auto t = chan.try_pull();
    EXPECT_EQ(t.status(), coro_util::qu_err::CLOSED);
    co_return;
  }());
}

} // namespace

#undef CATEGORY
