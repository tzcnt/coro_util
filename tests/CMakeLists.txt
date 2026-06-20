set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)

include(FetchContent)

# TooManyCooks (tmc) is a test-only dependency which provides task and executor functionality.
# Header-only INTERFACE target: TooManyCooks::TooManyCooks
FetchContent_Declare(
  TooManyCooks
  GIT_REPOSITORY https://github.com/tzcnt/TooManyCooks.git
  GIT_TAG main
)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2
)

# Don't override this project's compiler/linker settings on Windows.
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(gtest_hide_internal_symbols ON CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(TooManyCooks googletest)

include(GoogleTest)

# GoogleTest library itself (gtest-all.cc) emits a fair amount of warnings noise
target_compile_options(gtest PRIVATE "-w")

# this includes GoogleTest as SYSTEM, but some of its macros are expanded in user code,
# which triggers warnings anyway. disable these specific warnings that GoogleTest causes here.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # GCC gives weird object linkage errors with the local lambdas used in some tests
  # https://gcc.gnu.org/bugzilla/show_bug.cgi?id=114267
  add_compile_options("-Wno-subobject-linkage")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  add_compile_options(
    "-Wno-switch-default"
    "-Wno-weak-vtables"
  )
endif()

file(GLOB CORO_UTIL_TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(tests ${CORO_UTIL_TEST_SOURCES})

target_link_libraries(tests PRIVATE
  coro_util::coro_util
  TooManyCooks::TooManyCooks
  GTest::gtest
)

target_include_directories(tests PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${PROJECT_SOURCE_DIR}/include"
)

gtest_discover_tests(tests)
