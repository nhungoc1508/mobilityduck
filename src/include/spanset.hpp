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

struct SpansetTypes {
    static LogicalType intspanset();
    static LogicalType bigintspanset();
    static LogicalType floatspanset();
    static LogicalType textspanset();
    static LogicalType datespanset();
    static LogicalType tstzspanset();

    static const std::vector<LogicalType> &AllTypes();

    static void RegisterTypes(DatabaseInstance &db);
    static void RegisterCastFunctions(DatabaseInstance &db);
    static void RegisterScalarFunctions(DatabaseInstance &db);    
    static void RegisterSetUnnest(DatabaseInstance &db);    
};

struct SpansetFunctions{
    // for cast
    static bool Spanset_to_text(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool Text_to_spanset(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool Value_to_spanset(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

    // other    
    static void Spanset_as_text(DataChunk &args, ExpressionState &state, Vector &result);
    
    static void Spanset_constructor(DataChunk &args, ExpressionState &state, Vector &result);    
    static void Spanset_mem_size(DataChunk &args, ExpressionState &state, Vector &result);
    
};

struct SpansetTypeMapping {
    static meosType GetMeosTypeFromAlias(const std::string &alias);
    static LogicalType GetChildType(const LogicalType &type);
};

} // namespace duckdb
