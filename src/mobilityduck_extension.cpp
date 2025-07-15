#define DUCKDB_EXTENSION_MAIN

#include "mobilityduck_extension.hpp"
#include "types.hpp"
#include "functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void MobilityduckScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Mobilityduck " + name.GetString() + " 🐥");
	});
}

inline void MobilityduckOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Mobilityduck " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

// inline void ExecutePoint2D(DataChunk &args, ExpressionState &state, Vector &result) {
//     auto count = args.size();
//     auto &x = args.data[0];
//     auto &y = args.data[1];
//     x.Flatten(count);
//     y.Flatten(count);

//     auto &children = StructVector::GetEntries(result);
//     auto &x_child = children[0];
//     auto &y_child = children[1];
//     x_child->Reference(x);
//     y_child->Reference(y);

//     if (count == 1) {
//         result.SetVectorType(VectorType::CONSTANT_VECTOR);
//     }
// }

// inline void ExecuteTPoint2D(DataChunk &args, ExpressionState &state, Vector &result) {
// 	auto count = args.size();
// 	auto &x = args.data[0];
// 	auto &y = args.data[1];
// 	auto &t = args.data[2];
// 	x.Flatten(count);
// 	y.Flatten(count);
// 	t.Flatten(count);

// 	auto &children = StructVector::GetEntries(result);
// 	auto &x_child = children[0];
// 	auto &y_child = children[1];
// 	auto &t_child = children[2];
// 	x_child->Reference(x);
// 	y_child->Reference(y);
// 	t_child->Reference(t);

// 	if (count == 1) {
// 		result.SetVectorType(VectorType::CONSTANT_VECTOR);
// 	}
// }

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto mobilityduck_scalar_function = ScalarFunction("mobilityduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, MobilityduckScalarFun);
	ExtensionUtil::RegisterFunction(instance, mobilityduck_scalar_function);

	// Register another scalar function
	auto mobilityduck_openssl_version_scalar_function = ScalarFunction("mobilityduck_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, MobilityduckOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, mobilityduck_openssl_version_scalar_function);

	// Register geometry types
	GeoTypes::RegisterScalarFunctions(instance);
}

void MobilityduckExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string MobilityduckExtension::Name() {
	return "mobilityduck";
}

std::string MobilityduckExtension::Version() const {
#ifdef EXT_VERSION_MOBILITYDUCK
	return EXT_VERSION_MOBILITYDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void mobilityduck_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::MobilityduckExtension>();
}

DUCKDB_EXTENSION_API const char *mobilityduck_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
