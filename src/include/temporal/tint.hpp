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

#include "meos_wrapper_simple.hpp"

namespace duckdb {

class ExtensionLoader;

struct TInt {
    static LogicalType TINT();
    static void RegisterType(DatabaseInstance &instance);
    static void RegisterCastFunctions(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

} // namespace duckdb