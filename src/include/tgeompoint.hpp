#pragma once


#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

struct TGeomPointTypes {
    static LogicalType TGEOMPOINT();
    static LogicalType GEOMPOINT();
    static void RegisterTypes(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
    static void RegisterCastFunctions(DatabaseInstance &instance);
};

struct TgeomPointFunctions {
    static bool StringToTgeomPoint(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool StringToTgeomPointNew(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool TgeomPointToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool TgeomPointToStringNew(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
};



} // namespace duckdb