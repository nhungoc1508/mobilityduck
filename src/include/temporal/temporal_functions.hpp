#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class ExtensionLoader;

typedef struct {
    char *alias;
    meosType temptype;
} alias_type_struct;

struct TemporalHelpers {
    static meosType GetTemptypeFromAlias(const char *alias);
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
};

} // namespace duckdb