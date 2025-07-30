#pragma once

#include "common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

class ExtensionLoader;

struct GeoTypes {
    static LogicalType TINSTANT();
    static LogicalType TINT();
    static void RegisterTypes(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);

    static LogicalType TInstantType();
    static LogicalType TSequenceType();
    static LogicalType TSequenceSetType();
};

} // namespace duckdb