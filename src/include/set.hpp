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
    static LogicalType INTSET();
    static LogicalType BIGINTSET();
    static LogicalType FLOATSET();
    static LogicalType TEXTSET();
    static LogicalType DATESET();
    static LogicalType TSTZSET();

    static const std::vector<LogicalType> &AllTypes();

    static void RegisterTypes(DatabaseInstance &db);
	static void RegisterSet(DatabaseInstance &db);
	static void RegisterSetAsText(DatabaseInstance &db);
    static void RegisterCastFunctions(DatabaseInstance &db);
    static void RegisterSetConstructors(DatabaseInstance &db);
    static void RegisterSetConversion(DatabaseInstance &db);
    static void RegisterSetMemSize(DatabaseInstance &db);
    static void RegisterSetNumValues(DatabaseInstance &db);
    static void RegisterSetStartValue(DatabaseInstance &db);
    static void RegisterSetEndValue(DatabaseInstance &db);
    static void RegisterSetValueN(DatabaseInstance &db);
    static void RegisterSetGetValues(DatabaseInstance &db);
    static void RegisterSetUnnest(DatabaseInstance &db);
    
};

struct SetTypeMapping {
    static meosType GetMeosTypeFromAlias(const std::string &alias);
    static LogicalType GetChildType(const LogicalType &type);
};

} // namespace duckdb
