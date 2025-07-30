#pragma once

#include "common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "properties.hpp"
#include "types.hpp"

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>    
}

namespace duckdb {

class ExtensionLoader;

struct SpanTypes {
    static const std::vector<LogicalType> &AllTypes();

    static LogicalType INTSPAN();
    static LogicalType BIGINTSPAN();
    static LogicalType FLOATSPAN();
    static LogicalType TEXTSPAN();
    static LogicalType DATESPAN();
    static LogicalType TSTZSPAN();
    static void RegisterTypes(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

struct SpanTypeMapping {
    static meosType GetMeosTypeFromAlias(const std::string &alias);
    static LogicalType GetChildType(const LogicalType &type);
};

} // namespace duckdb