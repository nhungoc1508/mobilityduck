#define DUCKDB_EXTENSION_MAIN

#include "mobilityduck_extension.hpp"
#include "types.hpp"
#include "intset.hpp"
#include "set.hpp"
#include "geomset.hpp"

#include "functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

extern "C"{
	#include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
}

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

// MEOS
extern "C" {
	#include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
}

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

static void LoadInternal(DatabaseInstance &instance) {
	// Initialize MEOS
	meos_initialize();

	// Register a scalar function
	auto mobilityduck_scalar_function = ScalarFunction("mobilityduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, MobilityduckScalarFun);
	ExtensionUtil::RegisterFunction(instance, mobilityduck_scalar_function);

	// Register another scalar function
	auto mobilityduck_openssl_version_scalar_function = ScalarFunction("mobilityduck_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, MobilityduckOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, mobilityduck_openssl_version_scalar_function);

	// Register geometry types
	GeoTypes::RegisterScalarFunctions(instance);
	GeoTypes::RegisterTypes(instance);		

	//SetType
	SetTypes::RegisterTypes(instance);
	SetTypes::RegisterSet(instance);
	SetTypes::RegisterSetAsText(instance);
	SetTypes::RegisterSetConstructors(instance);
	SetTypes::RegisterSetConversion(instance);
	SetTypes::RegisterSetMemSize(instance);
	SetTypes::RegisterSetNumValues(instance);
	SetTypes::RegisterSetStartValue(instance);
	SetTypes::RegisterSetEndValue(instance);
	SetTypes::RegisterSetValueN(instance);
	SetTypes::RegisterSetGetValues(instance);
	SetTypes::RegisterSetUnnest(instance);

	//Geometry
	SpatialSetType::RegisterGeomSet(instance);
	SpatialSetType::RegisterGeomSetAsText(instance);
	SpatialSetType::RegisterMemSize(instance);
	SpatialSetType::RegisterGeogSet(instance);
	SpatialSetType::RegisterGeogSetAsText(instance);
	
	SpatialSetType::RegisterSRID(instance);
	SpatialSetType::RegisterSetSRID(instance);
	// SpatialSetType::RegisterTransform(instance); (debug later)

	SpatialSetType::RegisterStartValue(instance);
	SpatialSetType::RegisterEndValue(instance);
	
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
