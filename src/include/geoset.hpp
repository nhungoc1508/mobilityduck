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
    static void RegisterGeoSet(DatabaseInstance &db);        
    static void RegisterGeoSetAsText(DatabaseInstance &db);
    static void RegisterCastFunctions(DatabaseInstance &db);
    static void RegisterMemSize(DatabaseInstance &db);    
        
    //different from base set 

    static void RegisterSRID(DatabaseInstance &db);
    static void RegisterSetSRID(DatabaseInstance &db);
    static void RegisterTransform(DatabaseInstance &db); 

    // startValue 
    static void RegisterStartValue(DatabaseInstance &db);
    //endValue
    static void RegisterEndValue(DatabaseInstance &db);
    
};

} // namespace duckdb
