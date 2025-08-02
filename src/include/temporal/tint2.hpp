#pragma once

#include "common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_internal.h>
}

namespace duckdb {

class ExtensionLoader;

struct TInt2 {
    static LogicalType TInt2Make();
    static bool StringToTemporal(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool TemporalToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static void TInstantConstructor(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalSubtype(DataChunk &args, ExpressionState &state, Vector &result);
    static void TInstantValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void TInstantTimestamptz(DataChunk &args, ExpressionState &state, Vector &result);
    static void TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result);
    
    static vector<Value> TempArrToArray(Temporal **temparr, int32_t count, LogicalType element_type);

    static void RegisterType(DatabaseInstance &instance);
    static void RegisterCastFunctions(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

} // namespace duckdb