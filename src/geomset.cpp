// geometry.cpp
#include "geomset.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"


extern "C" {
    // #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>        
}

namespace duckdb {

//GeomSet
LogicalType SpatialSetType::GeomSet() {
    auto type = LogicalType(LogicalTypeId::VARCHAR);     
    type.SetAlias("geomSet");
    return type;
}

static void GeomSetParse(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            Set *s = set_in(str.GetString().c_str(), T_GEOMSET);
            char *cstr = set_out(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        }
    );	
}


void SpatialSetType::RegisterGeomSet(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"geomset", {LogicalType::VARCHAR}, SpatialSetType::GeomSet(), GeomSetParse));
}

static void GeomSetAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            Set *s = set_in(str.GetString().c_str(), T_GEOMSET);
            char *cstr = spatialset_as_text(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        }
    );	
}

void SpatialSetType::RegisterGeomSetAsText(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"asText", {SpatialSetType::GeomSet()}, LogicalType::VARCHAR, GeomSetAsText));
}

//Geography Set
LogicalType SpatialSetType::GeogSet() {
    auto type = LogicalType(LogicalTypeId::VARCHAR);     
    type.SetAlias("geogSet");
    return type;
}

static void GeogSetParse(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            Set *s = set_in(str.GetString().c_str(), T_GEOGSET);
            char *cstr = set_out(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        }
    );	
}


void SpatialSetType::RegisterGeogSet(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"geogset", {LogicalType::VARCHAR}, SpatialSetType::GeogSet(), GeogSetParse));
}

static void GeogSetAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            Set *s = set_in(str.GetString().c_str(), T_GEOGSET);
            char *cstr = spatialset_as_text(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        }
    );	
}

void SpatialSetType::RegisterGeogSetAsText(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"asText", {SpatialSetType::GeogSet()}, LogicalType::VARCHAR, GeogSetAsText));
}

//memSize

static void GeomMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_str) -> int32_t {                        
            Set *s = set_in(input_str.GetString().c_str(), T_GEOMSET);
            int size = VARSIZE_ANY(s);  // Get memory size
            free(s);
            return size;
        });
}

void SpatialSetType::RegisterMemSize(DatabaseInstance &db) {
    ExtensionUtil::RegisterFunction(
        db, ScalarFunction(
            "memSize",
            {SpatialSetType::GeomSet()},
            LogicalType::INTEGER,
            GeomMemSize));

}

static void SpatialSRID(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_str) -> int32_t {                        
            Set *s = set_in(input_str.GetString().c_str(), T_GEOMSET);
            int srid = spatialset_srid(s);
            free(s);
            return srid;
        });
}

void SpatialSetType::RegisterSRID(DatabaseInstance &db) {
    ExtensionUtil::RegisterFunction(
        db, ScalarFunction(
            "SRID",
            {SpatialSetType::GeomSet()},
            LogicalType::INTEGER,
            SpatialSRID));

}

// Set SRID 

static void SetSRID(DataChunk &args, ExpressionState &state, Vector &result_vec) {
	auto &input = args.data[0];
	auto &srid_vec = args.data[1];

	input.Flatten(args.size());
	srid_vec.Flatten(args.size());

	for (idx_t i = 0; i < args.size(); i++) {
	if (FlatVector::IsNull(input, i) || FlatVector::IsNull(srid_vec, i)) {
		FlatVector::SetNull(result_vec, i, true);
		continue;
	}

	std::string ewkt = FlatVector::GetData<string_t>(input)[i].GetString();
	int srid = FlatVector::GetData<int32_t>(srid_vec)[i];

	const char *ptr = ewkt.c_str();
	Set *set = set_in(ptr, T_GEOMSET);
	Set *modified = spatialset_set_srid(set, srid);
	char *out = set_out(modified, 15);
	
	string_t str = StringVector::AddString(result_vec, std::string(out));
	FlatVector::GetData<string_t>(result_vec)[i] = str;

	free(set);
	free(modified);
	free(out);
}

}

void SpatialSetType::RegisterSetSRID(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"set_srid",
		{SpatialSetType::GeomSet(), LogicalType::INTEGER},
		SpatialSetType::GeomSet(),
		SetSRID
	));
}

/*--- Transform -> something wrong in cmake, get back later ---*/

// static void TransformSpatialSet(DataChunk &args, ExpressionState &state, Vector &result_vec) {
// 	auto &input_vec = args.data[0];
// 	auto &srid_vec = args.data[1];

// 	input_vec.Flatten(args.size());
// 	srid_vec.Flatten(args.size());

// 	for (idx_t i = 0; i < args.size(); i++) {
// 		if (FlatVector::IsNull(input_vec, i) || FlatVector::IsNull(srid_vec, i)) {
// 			FlatVector::SetNull(result_vec, i, true);
// 			continue;
// 		}

// 		std::string ewkt = FlatVector::GetData<string_t>(input_vec)[i].GetString();
// 		int32_t srid = FlatVector::GetData<int32_t>(srid_vec)[i];
        
// 		const char *ptr = ewkt.c_str();        
// 		Set *s = set_in(ptr, T_GEOMSET);       
// 		Set *result = spatialset_transform(s, srid);
        
// 		free(s);

// 		if (!result) {
// 			FlatVector::SetNull(result_vec, i, true);
// 			continue;
// 		}

// 		char *out = set_out(result, 15);
// 		string_t str = StringVector::AddString(result_vec, std::string(out));
// 		FlatVector::GetData<string_t>(result_vec)[i] = str;        
// 		free(result);
// 		free(out);
// 	}
// }

// void SpatialSetType::RegisterTransform(DatabaseInstance &db) {
// 	ExtensionUtil::RegisterFunction(db, ScalarFunction(
// 		"transform",
// 		{SpatialSetType::GeomSet(), LogicalType::INTEGER},
// 		SpatialSetType::GeomSet(),
// 		TransformSpatialSet
// 	));
// }

    // startValue
static void GeomSetStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	input.Flatten(args.size());

	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(input, i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto ewkt = FlatVector::GetData<string_t>(input)[i].GetString();
		const char *ptr = ewkt.c_str();
		Set *s = set_in(ptr, T_GEOMSET);

		Datum d = set_start_value(s);  // returns geometry pointer
		GSERIALIZED *in = DatumGetGserializedP(d);  // serialize to WKT

        char *text = geo_as_text(in,6);

		string_t str = StringVector::AddString(result, std::string(text));
		FlatVector::GetData<string_t>(result)[i] = str;

		free(s);
		free(text);
	}
}

void SpatialSetType::RegisterStartValue(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"startValue",
		{SpatialSetType::GeomSet()},  // geomset as varchar
		LogicalType::VARCHAR,    // return geometry as WKT --> later can define the type 
		GeomSetStartValue
	));
}

    // endValue
static void GeomSetEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	input.Flatten(args.size());

	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(input, i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto ewkt = FlatVector::GetData<string_t>(input)[i].GetString();
		const char *ptr = ewkt.c_str();
		Set *s = set_in(ptr, T_GEOMSET);

		Datum d = set_end_value(s);  // returns geometry pointer
		GSERIALIZED *in = DatumGetGserializedP(d);  // serialize to WKT

        char *text = geo_as_text(in,6);

		string_t str = StringVector::AddString(result, std::string(text));
		FlatVector::GetData<string_t>(result)[i] = str;

		free(s);
		free(text);
	}
}

void SpatialSetType::RegisterEndValue(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"endValue",
		{SpatialSetType::GeomSet()},  // geomset as varchar
		LogicalType::VARCHAR,    // return geometry as WKT --> later can define the type 
		GeomSetEndValue
	));
}

} // namespace duckdb
