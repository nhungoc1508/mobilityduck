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
    static LogicalType intset();
    static LogicalType bigintset();
    static LogicalType floatset();
    static LogicalType textset();
    static LogicalType dateset();
    static LogicalType tstzset();

    static const std::vector<LogicalType> &AllTypes();

    static void RegisterTypes(DatabaseInstance &db);
    static void RegisterCastFunctions(DatabaseInstance &db);
    static void RegisterScalarFunctions(DatabaseInstance &db);    
    static void RegisterSetUnnest(DatabaseInstance &db);    
};

struct SetFunctions{
    // for cast
    static bool Set_to_text(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool Text_to_set(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool Value_to_set(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

    // other    
    static void Set_as_text(DataChunk &args, ExpressionState &state, Vector &result);
    
    static void Set_constructor(DataChunk &args, ExpressionState &state, Vector &result);    
    static void Set_mem_size(DataChunk &args, ExpressionState &state, Vector &result);
    static void Set_num_values(DataChunk &args, ExpressionState &state, Vector &result);
    static void Set_start_value(DataChunk &args, ExpressionState &state, Vector &result);
    static void Set_end_value(DataChunk &args, ExpressionState &state, Vector &result);
    static void Set_value_n(DataChunk &args, ExpressionState &state, Vector &result_vec);
    static void Set_values(DataChunk &args, ExpressionState &state, Vector &result);
};

struct SetTypeMapping {
    static meosType GetMeosTypeFromAlias(const std::string &alias);
    static LogicalType GetChildType(const LogicalType &type);
};

} // namespace duckdb
