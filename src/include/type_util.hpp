#pragma once

#include "meos_wrapper_simple.hpp"

#define TIMESTAMP_ADAPT_GAP_MS (30LL * 365LL + 7LL) * 24LL * 60LL * 60LL * 1000000LL
#define OUT_DEFAULT_DECIMAL_DIGITS 15

namespace duckdb {

struct DataHelpers {
    static Datum getDatum(string_t arg);
};

} // namespace duckdb