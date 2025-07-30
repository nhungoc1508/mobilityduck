#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {
    struct Spatial{
        static LogicalType Geometry();
        static LogicalType Geography();
        static LogicalType Point();
        static LogicalType Line();
        static LogicalType lseg();
        static LogicalType Polygon();
        static LogicalType Box();
        static LogicalType Circle();
        static LogicalType Path();
                
        static void RegisterSpatialType(DatabaseInstance &db);
        static void RegisterPointConstructor(DatabaseInstance &db);
    };
}