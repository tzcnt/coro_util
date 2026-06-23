#!/usr/bin/env bash
# Generate a coverage report for the coro_util library using llvm tooling.
#
# coro_util is header-only, so coverage is collected by instrumenting the test
# executable (which #includes the headers) and scoping the report to the five
# public headers in ./include/coro_util.
#
# Usage:
#   ./coverage.sh                 # generate a coverage summary (lcov + report)
#   ./coverage.sh <FILE>          # also show line-by-line coverage for <FILE>
#                                 # (path relative to repo root, or a bare header
#                                 #  name resolved under ./include/coro_util)
#
# Configuration via environment variables:
#   BUILD_DIR  (default: build/coverage)
#   CXX        (default: clang++)
#   CC         (default: clang)
#
# Requires:
#   llvm-profdata (any version), llvm-cov (any version), cmake, clang++.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BUILD_DIR="${BUILD_DIR:-build/coverage}"
CXX="${CXX:-clang++}"
CC="${CC:-clang}"

# The headers whose coverage we care about: the public headers in the root of
# ./include/coro_util (the  subdirectory is excluded). The lcov export,
# the summary report and the line-by-line show are all scoped to these.
COVERAGE_SOURCES=(./include/coro_util/*.hpp)

# ---------------------------------------------------------------------------
# Locate llvm tools. Prefer plain names, then the newest available numbered
# variant (matching the local installation, which may be newer than CI's).
# ---------------------------------------------------------------------------
find_llvm_tool() {
    local base="$1"
    if command -v "$base" >/dev/null 2>&1; then
        echo "$base"
        return 0
    fi
    # Look for numbered variants llvm-cov-21, llvm-cov-20, ...
    local found
    found=$(compgen -c "${base}-" 2>/dev/null \
        | grep -E "^${base}-[0-9]+$" \
        | sort -t- -k2,2 -n -r \
        | head -n1 || true)
    if [[ -n "$found" ]]; then
        echo "$found"
        return 0
    fi
    return 1
}

LLVM_PROFDATA=$(find_llvm_tool llvm-profdata) || {
    echo "ERROR: llvm-profdata not found. Install llvm tooling and try again." >&2
    exit 1
}
LLVM_COV=$(find_llvm_tool llvm-cov) || {
    echo "ERROR: llvm-cov not found. Install llvm tooling and try again." >&2
    exit 1
}
echo "Using: $LLVM_PROFDATA"
echo "Using: $LLVM_COV"
echo "Using: $CXX"
echo "Using: $CC"

# ---------------------------------------------------------------------------
# Configure + build with coverage instrumentation. coro_util is header-only,
# so the flags are applied to the test target's translation units.
# ---------------------------------------------------------------------------
COV_FLAGS="-fprofile-instr-generate -fcoverage-mapping"

cmake \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_COMPILER="${CC}" \
    -DCMAKE_CXX_COMPILER="${CXX}" \
    -DCMAKE_CXX_FLAGS="${COV_FLAGS}" \
    -DCMAKE_EXE_LINKER_FLAGS="${COV_FLAGS}" \
    -B "${BUILD_DIR}" \
    .

cmake --build "${BUILD_DIR}" \
    --parallel "$(nproc)" \
    --target tests

# ---------------------------------------------------------------------------
# Run tests and merge raw profiles.
# ---------------------------------------------------------------------------
PROF_DIR="${BUILD_DIR}/coverage-profiles"
rm -rf "${PROF_DIR}"
mkdir -p "${PROF_DIR}"

TESTS_BIN="${BUILD_DIR}/tests/tests"

LLVM_PROFILE_FILE="${PROF_DIR}/tests.profraw" \
    "${TESTS_BIN}"

"${LLVM_PROFDATA}" merge \
    -o "${PROF_DIR}/coverage.profdata" \
    "${PROF_DIR}/tests.profraw"

# ---------------------------------------------------------------------------
# Emit reports. Always produce an lcov file scoped to coro_util headers;
# optionally show line-by-line coverage for a specific file.
# ---------------------------------------------------------------------------
LCOV_FILE="${BUILD_DIR}/coverage.lcov"
"${LLVM_COV}" export \
    -format=lcov \
    -instr-profile "${PROF_DIR}/coverage.profdata" \
    -object "${TESTS_BIN}" \
    -sources "${COVERAGE_SOURCES[@]}" \
    > "${LCOV_FILE}"
echo "Wrote lcov coverage to ${LCOV_FILE}"

echo
echo "===== Coverage summary (coro_util headers) ====="
"${LLVM_COV}" report \
    -instr-profile "${PROF_DIR}/coverage.profdata" \
    -object "${TESTS_BIN}" \
    -sources "${COVERAGE_SOURCES[@]}"

if [[ $# -ge 1 ]]; then
    TARGET_FILE="$1"
    if [[ ! -f "${TARGET_FILE}" ]]; then
        # Try resolving a bare header name under the coro_util include dir.
        for candidate in \
            "include/coro_util/${TARGET_FILE}"
        do
            if [[ -f "${candidate}" ]]; then
                TARGET_FILE="${candidate}"
                break
            fi
        done
    fi
    if [[ ! -f "${TARGET_FILE}" ]]; then
        echo "ERROR: source file '$1' not found." >&2
        exit 1
    fi

    ABS_TARGET="$(realpath "${TARGET_FILE}")"
    OUT_FILE="${BUILD_DIR}/coverage-$(basename "${TARGET_FILE}").txt"

    echo
    echo "===== Line-by-line coverage for ${TARGET_FILE} ====="
    "${LLVM_COV}" show \
        -instr-profile "${PROF_DIR}/coverage.profdata" \
        -object "${TESTS_BIN}" \
        -show-line-counts-or-regions \
        -use-color=false \
        -sources "${ABS_TARGET}" \
        > "${OUT_FILE}"
    cat "${OUT_FILE}"
    echo
    echo "Wrote line-by-line coverage to ${OUT_FILE}"
fi
