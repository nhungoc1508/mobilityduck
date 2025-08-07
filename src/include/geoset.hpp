#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

struct SpatialSetType{    
    static LogicalType GeomSet();     
    static LogicalType GeogSet();   
    static LogicalType WKB_BLOB();

    static void RegisterTypes(DatabaseInstance &db);
    static void RegisterCastFunctions(DatabaseInstance &db);
    static void RegisterScalarFunctions(DatabaseInstance &db);        
};

struct SpatialSetFunctions{
    //cast
    static bool GeoSetToText(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool TextToGeoSet(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

    //other
    static void GeoSetFromText(DataChunk &args, ExpressionState &state, Vector &result); 
    static void GeoSetAsText(DataChunk &args, ExpressionState &state, Vector &result);
    static void GeomMemSize(DataChunk &args, ExpressionState &state, Vector &result);
    static void SpatialSRID(DataChunk &args, ExpressionState &state, Vector &result);
    static void SetSRID(DataChunk &args, ExpressionState &state, Vector &result_vec);
    static void TransformSpatialSet(DataChunk &args, ExpressionState &state, Vector &result_vec);
    static void GeoSetStartValue(DataChunk &args, ExpressionState &state, Vector &result);
};   

} // namespace duckdb
