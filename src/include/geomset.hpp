#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

struct SpatialSetType{
    //geometry set
    static LogicalType GeomSet();     
    static void RegisterGeomSet(DatabaseInstance &db);        
    static void RegisterGeomSetAsText(DatabaseInstance &db);
    static void RegisterMemSize(DatabaseInstance &db);    
    
    //geography set
    static LogicalType GeogSet();   
    static void RegisterGeogSet(DatabaseInstance &db);        
    static void RegisterGeogSetAsText(DatabaseInstance &db);

    //different from base set 

    static void RegisterSRID(DatabaseInstance &db);
    static void RegisterSetSRID(DatabaseInstance &db);
    
};

} // namespace duckdb
