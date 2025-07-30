// #pragma once

// #include "duckdb/common/exception.hpp"
// #include "duckdb/common/string_util.hpp"
// #include "duckdb/function/scalar_function.hpp"
// #include "duckdb/main/extension_util.hpp"
// #include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
// #include "properties.hpp"
// namespace duckdb {

// struct IntSetType {
// 	static LogicalType INTSET();
// 	static void RegisterTypes(DatabaseInstance &db);
// 	//input: string
// 	static void RegisterIntSet(DatabaseInstance &db);
// 	//input: list
// 	static void RegisterIntSetConstructor(DatabaseInstance &db);
// 	//output asText
// 	static void RegisterIntSetAsText(DatabaseInstance &db);
// 	//conversion function
// 	static void RegisterIntToSet(DatabaseInstance &db);
// 	//memSize
// 	static void RegisterIntSetMemSize(DatabaseInstance &db);
// 	//numValue
// 	static void RegisterIntSetNumValues(DatabaseInstance &db);
// 	//startValue
// 	static void RegisterIntSetStartValue(DatabaseInstance &db);
// 	//endValue
// 	static void RegisterIntSetEndValue(DatabaseInstance &db);
// 	//valueN
// 	static void RegisterIntSetValueN(DatabaseInstance &db);
// 	//getValues
// 	static void RegisterIntSetGetValues(DatabaseInstance &db);
// 	//shift
// 	static void RegisterIntSetShift(DatabaseInstance &db);
// 	//unnest 
// 	static void RegisterIntSetUnnest(DatabaseInstance &db);
// };

// } // namespace duckdb