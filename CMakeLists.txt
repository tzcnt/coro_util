cmake_minimum_required(VERSION 3.21) # PROJECT_IS_TOP_LEVEL

project(coro_util
  VERSION 0.1.0
  DESCRIPTION "Async coroutine utilities"
  LANGUAGES CXX
)

# ---- Header-only library target ----
add_library(coro_util INTERFACE)
add_library(coro_util::coro_util ALIAS coro_util)

target_include_directories(coro_util INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)

target_compile_features(coro_util INTERFACE cxx_std_20)

# ---- Tests ----
# Only built when coro_util is the top-level project (i.e. configured directly,
# not pulled in via add_subdirectory() / FetchContent by a consumer). This keeps
# the test-only dependencies (GoogleTest, TooManyCooks) off a consumer's build.
option(CORO_UTIL_BUILD_TESTS "Build the coro_util test suite" ${PROJECT_IS_TOP_LEVEL})

if(CORO_UTIL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
