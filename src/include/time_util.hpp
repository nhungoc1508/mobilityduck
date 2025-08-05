#pragma once

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

constexpr int32_t EPOCH_OFFSET_DAYS = 10957;
constexpr int64_t EPOCH_OFFSET_MICROS = EPOCH_OFFSET_DAYS * 86400000000;

// DuckDB → MEOS
inline int32_t ToMeosDate(duckdb::date_t d) {
    return static_cast<int32_t>(d) - EPOCH_OFFSET_DAYS;
}

inline int64_t ToMeosTimestamp(duckdb::timestamp_t ts) {
    return ts.value - EPOCH_OFFSET_MICROS;
}

// MEOS → DuckDB
inline duckdb::date_t FromMeosDate(int32_t pg_date) {
    return duckdb::date_t(pg_date + EPOCH_OFFSET_DAYS);
}

inline duckdb::timestamp_t FromMeosTimestamp(int64_t pg_ts) {
    return duckdb::timestamp_t(pg_ts + EPOCH_OFFSET_MICROS);
}

} 
