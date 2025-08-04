#pragma once

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckmeos {

constexpr int32_t POSTGRES_EPOCH_OFFSET_DAYS = 10957;
constexpr int64_t POSTGRES_EPOCH_OFFSET_MICROS = 946684800000000LL;

// DuckDB → MEOS
inline int32_t ToPostgresDate(duckdb::date_t d) {
    return static_cast<int32_t>(d) - POSTGRES_EPOCH_OFFSET_DAYS;
}

inline int64_t ToPostgresTimestamp(duckdb::timestamp_t ts) {
    return ts.value - POSTGRES_EPOCH_OFFSET_MICROS;
}

// MEOS → DuckDB
inline duckdb::date_t FromPostgresDate(int32_t pg_date) {
    return duckdb::date_t(pg_date + POSTGRES_EPOCH_OFFSET_DAYS);
}

inline duckdb::timestamp_t FromPostgresTimestamp(int64_t pg_ts) {
    return duckdb::timestamp_t(pg_ts + POSTGRES_EPOCH_OFFSET_MICROS);
}

} // namespace duckmeos
