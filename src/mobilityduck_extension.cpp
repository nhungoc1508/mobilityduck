#define DUCKDB_EXTENSION_MAIN

#include "mobilityduck_extension.hpp"
#include "temporal/temporal_functions.hpp"
#include "temporal/temporal.hpp"
#include "temporal/tbox.hpp"
#include "geo/stbox.hpp"
#include "geo/tgeompoint.hpp"
#include "duckdb.hpp"
// #include "tgeompoint.hpp"
#include "temporal/span.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "index/rtree_module.hpp"
#include <mutex>

extern "C"{	
    #include <meos.h>
}

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

static void LoadInternal(DatabaseInstance &instance) {
	// Initialize MEOS
	static std::once_flag meos_init_flag;
    std::call_once(meos_init_flag, []() {
        meos_initialize();
	
    });

	Connection con(instance);

	// con.Query("INSTALL spatial;");
	// con.Query("LOAD spatial;");

	// Register a scalar function
	auto mobilityduck_scalar_function = ScalarFunction("mobilityduck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, MobilityduckScalarFun);
	ExtensionUtil::RegisterFunction(instance, mobilityduck_scalar_function);

	// Register another scalar function
	auto mobilityduck_openssl_version_scalar_function = ScalarFunction("mobilityduck_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, MobilityduckOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, mobilityduck_openssl_version_scalar_function);


	StboxType::RegisterType(instance);
	StboxType::RegisterCastFunctions(instance);
	StboxType::RegisterScalarFunctions(instance);
  
  	SpanTypes::RegisterScalarFunctions(instance);
	SpanTypes::RegisterTypes(instance);
	SpanTypes::RegisterCastFunctions(instance);

	RTreeModule::RegisterRTreeIndex(instance);
	RTreeModule::RegisterIndexScan(instance);
	RTreeModule::RegisterScanOptimizer(instance);
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
