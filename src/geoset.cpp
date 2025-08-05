#include "geoset.hpp"
#include "tydef.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"


extern "C" {    
    #include "meos.h"    
    #include "meos_internal.h"   
    #include "meos_geo.h"        
}

namespace duckdb {

//GeomSet
LogicalType SpatialSetType::GeomSet() {
    auto type = LogicalType(LogicalTypeId::BLOB);     
    type.SetAlias("geomSet");
    return type;
}

LogicalType SpatialSetType::GeogSet() {
    auto type = LogicalType(LogicalTypeId::BLOB);     
    type.SetAlias("geogSet");
    return type;
}

static void GeoSetFromText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            auto type = result.GetType();
            auto set_type = (type == SpatialSetType::GeomSet()) ? T_GEOMSET:T_GEOGSET; 
            Set *s = set_in(str.GetString().c_str(), set_type);
            size_t total_size = VARSIZE(s); 
            string_t blob = StringVector::AddStringOrBlob(result, (const char*)s, total_size);        
            free(s);
            return blob;                     
        }
    );	
}

void SpatialSetType::RegisterGeoSet(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"geomset", {LogicalType::VARCHAR}, SpatialSetType::GeomSet(), GeoSetFromText));
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"geogset", {LogicalType::VARCHAR}, SpatialSetType::GeogSet(), GeoSetFromText));
}

static void GeoSetAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            const uint8_t *data = (const uint8_t *)str.GetData();
            size_t size = str.GetSize();
            
            Set *s = (Set*)malloc(size);
            memcpy(s, data, size);

            char *cstr = spatialset_as_text(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        }
    );	
}

void SpatialSetType::RegisterGeoSetAsText(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"asText", {SpatialSetType::GeomSet()}, LogicalType::VARCHAR, GeoSetAsText));
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"asText", {SpatialSetType::GeogSet()}, LogicalType::VARCHAR, GeoSetAsText));
}

// Cast Function 

bool GeoSetToText(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    auto result_data = FlatVector::GetData<string_t>(result); 

    for (idx_t i = 0; i < count; ++i) {
        if (FlatVector::IsNull(source, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        Value val = source.GetValue(i);
        const string_t &blob = StringValue::Get(val);
        const uint8_t *data = (const uint8_t *)(blob.GetData());
        size_t size = blob.GetSize();

        Set *s = (Set*)malloc(size);
        memcpy(s, data, size);        

        char *cstr = spatialset_as_text(s, 15); 

        result_data[i] = StringVector::AddString(result, cstr);

        free(cstr);
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}

bool TextToGeoSet(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);

    auto target_type = result.GetType();    
    auto input_data = FlatVector::GetData<string_t>(source);
    auto result_data = FlatVector::GetData<string_t>(result);

    for (idx_t i = 0; i < count; ++i) {
        if (FlatVector::IsNull(source, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        const std::string input_blob = input_data[i].GetString();
        auto set_type = (target_type == SpatialSetType::GeomSet()) ? T_GEOMSET:T_GEOGSET;
        Set *s = set_in(input_blob.c_str(), set_type);        
        size_t total_size = VARSIZE(s); 
        result_data[i] = StringVector::AddStringOrBlob(result, (const char*)s, total_size);        
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}


void SpatialSetType::RegisterCastFunctions(DatabaseInstance &instance) {    
    ExtensionUtil::RegisterCastFunction(
        instance,
        SpatialSetType::GeomSet(),                      
        LogicalType::VARCHAR,   
        GeoSetToText
    ); // Blob to text
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR, 
        SpatialSetType::GeomSet(),                                    
        TextToGeoSet   
    ); // text to blob
    ExtensionUtil::RegisterCastFunction(
        instance,
        SpatialSetType::GeogSet(),                      
        LogicalType::VARCHAR,   
        GeoSetToText
    ); // Blob to text
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR, 
        SpatialSetType::GeogSet(),                                    
        TextToGeoSet   
    ); // text to blob
 
}

//memSize

static void GeomMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> int32_t {                        
            const uint8_t *data = (const uint8_t *)input_blob.GetData();
            size_t size = input_blob.GetSize();
            Set *s = (Set*)malloc(size);
            memcpy(s, data, size);
            int mem_size = set_mem_size(s);  // Get memory size
            free(s);
            return mem_size;             
        });
}

void SpatialSetType::RegisterMemSize(DatabaseInstance &db) {
    ExtensionUtil::RegisterFunction(
        db, ScalarFunction(
            "memSize",
            {SpatialSetType::GeomSet()},
            LogicalType::INTEGER,
            GeomMemSize));
    ExtensionUtil::RegisterFunction(
        db, ScalarFunction(
            "memSize",
            {SpatialSetType::GeogSet()},
            LogicalType::INTEGER,
            GeomMemSize));

}

static void SpatialSRID(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> int32_t {                        
            const uint8_t *data = (const uint8_t *)input_blob.GetData();
            size_t size = input_blob.GetSize();
            Set *s = (Set*)malloc(size);
            memcpy(s, data, size);
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
    ExtensionUtil::RegisterFunction(
        db, ScalarFunction(
            "SRID",
            {SpatialSetType::GeogSet()},
            LogicalType::INTEGER,
            SpatialSRID));

}

// Set SRID 

static void SetSRID(DataChunk &args, ExpressionState &state, Vector &result_vec) {
	auto &input = args.data[0];
	auto &srid_vec = args.data[1];

	input.Flatten(args.size());
	srid_vec.Flatten(args.size());

    auto input_data = FlatVector::GetData<string_t>(input);
    auto srid_data = FlatVector::GetData<int32_t>(srid_vec);
    auto result_data = FlatVector::GetData<string_t>(result_vec);


	for (idx_t i = 0; i < args.size(); i++) {
        if (FlatVector::IsNull(input, i) || FlatVector::IsNull(srid_vec, i)) {
            FlatVector::SetNull(result_vec, i, true);
            continue;
        }
        const string_t &blob = input_data[i];
        const uint8_t *data = (const uint8_t *)blob.GetData();
        size_t size = blob.GetSize();
        
        Set *set = (Set *)malloc(size);
        memcpy(set, data, size);
        
        Set *modified = spatialset_set_srid(set, srid_data[i]);

        size_t total_size = VARSIZE(modified); 
        result_data[i] = StringVector::AddStringOrBlob(result_vec, (const char*)modified, total_size);                

        free(set);
        free(modified);        
    }

}

void SpatialSetType::RegisterSetSRID(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"set_srid",
		{SpatialSetType::GeomSet(), LogicalType::INTEGER},
		SpatialSetType::GeomSet(),
		SetSRID
	));
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"set_srid",
		{SpatialSetType::GeogSet(), LogicalType::INTEGER},
		SpatialSetType::GeogSet(),
		SetSRID
	));
}

/*--- Transform -> something wrong in cmake, get back later ---*/

static void TransformSpatialSet(DataChunk &args, ExpressionState &state, Vector &result_vec) {
	auto &input_vec = args.data[0];
	auto &srid_vec = args.data[1];

	input_vec.Flatten(args.size());
	srid_vec.Flatten(args.size());

    auto input_data = FlatVector::GetData<string_t>(input_vec);
    auto srid_data = FlatVector::GetData<int32_t>(srid_vec);
    auto result_data = FlatVector::GetData<string_t>(result_vec);


	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(input_vec, i) || FlatVector::IsNull(srid_vec, i)) {
			FlatVector::SetNull(result_vec, i, true);
			continue;
		}

        const string_t &blob = input_data[i];
        const uint8_t *data = (const uint8_t *)blob.GetData();
        size_t size = blob.GetSize();
        
        Set *s = (Set *)malloc(size);
        memcpy(s, data, size);        		     
		Set *result = spatialset_transform(s, srid_data[i]);
        
		free(s);

		if (!result) {
			FlatVector::SetNull(result_vec, i, true);
			continue;
		}
        size_t total_size = VARSIZE(result); 
        result_data[i] = StringVector::AddStringOrBlob(result_vec, (const char*)result, total_size);                        
        free(result);		
	}
}

void SpatialSetType::RegisterTransform(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"transform",
		{SpatialSetType::GeomSet(), LogicalType::INTEGER},
		SpatialSetType::GeomSet(),
		TransformSpatialSet
	));
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"transform",
		{SpatialSetType::GeogSet(), LogicalType::INTEGER},
		SpatialSetType::GeogSet(),
		TransformSpatialSet
	));
}

// // startValue
// static void GeomSetStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
// 	auto &input = args.data[0];
// 	input.Flatten(args.size());

// 	for (idx_t i = 0; i < args.size(); i++) {
// 		if (FlatVector::IsNull(input, i)) {
// 			FlatVector::SetNull(result, i, true);
// 			continue;
// 		}

// 		auto ewkt = FlatVector::GetData<string_t>(input)[i].GetString();
// 		const char *ptr = ewkt.c_str();
// 		Set *s = set_in(ptr, T_GEOMSET);

// 		Datum d = set_start_value(s);  // returns geometry pointer
// 		GSERIALIZED *in = DatumGetGserializedP(d);  // serialize to WKT

//         char *text = geo_as_text(in,6);

// 		string_t str = StringVector::AddString(result, std::string(text));
// 		FlatVector::GetData<string_t>(result)[i] = str;

// 		free(s);
// 		free(text);
// 	}
// }

// void SpatialSetType::RegisterStartValue(DatabaseInstance &db) {
// 	ExtensionUtil::RegisterFunction(db, ScalarFunction(
// 		"startValue",
// 		{SpatialSetType::GeomSet()},  // geomset as varchar
// 		LogicalType::VARCHAR,    // return geometry as WKT --> later can define the type 
// 		GeomSetStartValue
// 	));
// }

//     // endValue
// static void GeomSetEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
// 	auto &input = args.data[0];
// 	input.Flatten(args.size());

// 	for (idx_t i = 0; i < args.size(); i++) {
// 		if (FlatVector::IsNull(input, i)) {
// 			FlatVector::SetNull(result, i, true);
// 			continue;
// 		}

// 		auto ewkt = FlatVector::GetData<string_t>(input)[i].GetString();
// 		const char *ptr = ewkt.c_str();
// 		Set *s = set_in(ptr, T_GEOMSET);

// 		Datum d = set_end_value(s);  // returns geometry pointer
// 		GSERIALIZED *in = DatumGetGserializedP(d);  // serialize to WKT

//         char *text = geo_as_text(in,6);

// 		string_t str = StringVector::AddString(result, std::string(text));
// 		FlatVector::GetData<string_t>(result)[i] = str;

// 		free(s);
// 		free(text);
// 	}
// }

// void SpatialSetType::RegisterEndValue(DatabaseInstance &db) {
// 	ExtensionUtil::RegisterFunction(db, ScalarFunction(
// 		"endValue",
// 		{SpatialSetType::GeomSet()},  // geomset as varchar
// 		LogicalType::VARCHAR,    // return geometry as WKT --> later can define the type 
// 		GeomSetEndValue
// 	));
// }

} // namespace duckdb
