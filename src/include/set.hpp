#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <tydef.hpp>

extern "C" {    
    #include <meos.h>    
    #include <meos_internal.h>    
}

namespace duckdb {

struct SetTypes {
    static LogicalType INTSET();
    static LogicalType BIGINTSET();
    static LogicalType FLOATSET();
    static LogicalType TEXTSET();
    static LogicalType DATESET();
    static LogicalType TSTZSET();

    static const std::vector<LogicalType> &AllTypes();

    static void RegisterTypes(DatabaseInstance &db);
    static void RegisterCastFunctions(DatabaseInstance &db);
    static void RegisterScalarFunctions(DatabaseInstance &db);    
    static void RegisterSetUnnest(DatabaseInstance &db);    
};

struct SetFunctions{
    // for cast
    static bool SetToText(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool TextToSet(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

    // other
    static void SetFromText(DataChunk &args, ExpressionState &state, Vector &result);
    static void AsText(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetConstructor(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetConversion(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetMemSize(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetNumValues(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetStartValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetEndValue(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetValueN(DataChunk &args, ExpressionState &state, Vector &result_vec);
    static void GetSetValues(DataChunk &args, ExpressionState &state, Vector &result);
};

struct SetTypeMapping {
    static meosType GetMeosTypeFromAlias(const std::string &alias);
    static LogicalType GetChildType(const LogicalType &type);
};

} // namespace duckdb
