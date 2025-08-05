#pragma once

#include "meos_wrapper_simple.hpp"
#include "duckdb/common/typedefs.hpp"

#include "span.hpp"
#include "set.hpp"

namespace duckdb {

class ExtensionLoader;

typedef struct {
    char *alias;
    meosType temptype;
} alias_type_struct;

#define TIMESTAMP_ADAPT_GAP_MS (30LL * 365LL + 7LL) * 24LL * 60LL * 60LL * 1000000LL
#define OUT_DEFAULT_DECIMAL_DIGITS 15

struct TemporalHelpers {
    static meosType GetTemptypeFromAlias(const char *alias);
    static interval_t MeosToDuckDBInterval(MeosInterval *interval);
    static vector<Value> TempArrToArray(Temporal **temparr, int32_t count, LogicalType element_type);
    static LogicalType GetTemporalLogicalType(meosType temptype);
    static timestamp_tz_t DuckDBToMeosTimestamp(timestamp_tz_t duckdb_ts);
    static timestamp_tz_t MeosToDuckDBTimestamp(timestamp_tz_t meos_ts);
};

struct TemporalFunctions {
    // Cast functions
    static bool StringToTemporal(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool TemporalToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

    // Scalar functions
    static void TInstantConstructor(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalSubtype(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalInterp(DataChunk &args, ExpressionState &state, Vector &result);
    static void TInstantValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalStartValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalEndValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalMinValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalMaxValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalValueN(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalMinInstant(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalMaxInstant(DataChunk &args, ExpressionState &state, Vector &result);
    static void TInstantTimestamptz(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalDuration(DataChunk &args, ExpressionState &state, Vector &result);
    static void TsequenceConstructor(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalToTsequence(DataChunk &args, ExpressionState &state, Vector &result);
    static void TsequencesetConstructor(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalToTsequenceset(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalToTstzspan(DataChunk &args, ExpressionState &state, Vector &result);
    static void TnumberToSpan(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalValueset(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result);
    static void TnumberShiftValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TnumberScaleValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TnumberShiftScaleValue(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb