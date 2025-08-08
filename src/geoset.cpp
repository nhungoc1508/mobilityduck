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
    #include "meos_internal_geo.h"     
}

namespace duckdb {

//GeomSet
LogicalType SpatialSetType::geomset() {
    auto type = LogicalType(LogicalTypeId::BLOB);     
    type.SetAlias("geomset");
    return type;
}

LogicalType SpatialSetType::WKB_BLOB(){
    auto type = LogicalType(LogicalTypeId::BLOB);     
    type.SetAlias("WKB_BLOB");
    return type;
}

LogicalType SpatialSetType::geogset() {
    auto type = LogicalType(LogicalTypeId::BLOB);     
    type.SetAlias("geogset");
    return type;
}

void SpatialSetType::RegisterTypes(DatabaseInstance &db){
    ExtensionUtil::RegisterType(db, "geomset", geomset());
    ExtensionUtil::RegisterType(db, "geogset", geogset());
}

void SpatialSetFunctions::Spatialset_as_text(DataChunk &args, ExpressionState &state, Vector &result) {
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

// Cast Function 

bool SpatialSetFunctions::Geoset_to_text(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
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

bool SpatialSetFunctions::Text_to_geoset(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
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
        auto set_type = (target_type == SpatialSetType::geomset()) ? T_GEOMSET:T_GEOGSET;
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
        SpatialSetType::geomset(),                      
        LogicalType::VARCHAR,   
        SpatialSetFunctions::Geoset_to_text
    ); // Blob to text
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR, 
        SpatialSetType::geomset(),                                    
        SpatialSetFunctions::Text_to_geoset   
    ); // text to blob
    ExtensionUtil::RegisterCastFunction(
        instance,
        SpatialSetType::geogset(),                      
        LogicalType::VARCHAR,   
        SpatialSetFunctions::Geoset_to_text
    ); // Blob to text
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR, 
        SpatialSetType::geogset(),                                    
        SpatialSetFunctions::Text_to_geoset   
    ); // text to blob
 
}

//memSize

void SpatialSetFunctions::Set_mem_size(DataChunk &args, ExpressionState &state, Vector &result) {
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

void SpatialSetFunctions::Spatialset_srid(DataChunk &args, ExpressionState &state, Vector &result) {
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

// Set SRID 

void SpatialSetFunctions::Spatialset_set_srid(DataChunk &args, ExpressionState &state, Vector &result_vec) {
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

void SpatialSetFunctions::Spatialset_transform(DataChunk &args, ExpressionState &state, Vector &result_vec) {
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
void SpatialSetType::RegisterScalarFunctions(DatabaseInstance &db) {	    
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"asText", {SpatialSetType::geomset()}, LogicalType::VARCHAR, SpatialSetFunctions::Spatialset_as_text));
    
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"asText", {SpatialSetType::geogset()}, LogicalType::VARCHAR, SpatialSetFunctions::Spatialset_as_text));

    ExtensionUtil::RegisterFunction(db, ScalarFunction(
        "memSize", {SpatialSetType::geomset()}, LogicalType::INTEGER, SpatialSetFunctions::Set_mem_size));
    
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
        "memSize", {SpatialSetType::geogset()}, LogicalType::INTEGER, SpatialSetFunctions::Set_mem_size));

    ExtensionUtil::RegisterFunction(db, ScalarFunction(
        "SRID", {SpatialSetType::geomset()}, LogicalType::INTEGER, SpatialSetFunctions::Spatialset_srid));
    
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
        "SRID", {SpatialSetType::geogset()}, LogicalType::INTEGER, SpatialSetFunctions::Spatialset_srid));

    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"setSRID", {SpatialSetType::geomset(), LogicalType::INTEGER}, SpatialSetType::geomset(), SpatialSetFunctions::Spatialset_set_srid));

    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"setSRID", {SpatialSetType::geogset(), LogicalType::INTEGER}, SpatialSetType::geogset(), SpatialSetFunctions::Spatialset_set_srid));

    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"transform", {SpatialSetType::geomset(), LogicalType::INTEGER}, SpatialSetType::geomset(), SpatialSetFunctions::Spatialset_transform));
    
    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"transform", {SpatialSetType::geogset(), LogicalType::INTEGER}, SpatialSetType::geogset(), SpatialSetFunctions::Spatialset_transform));

    ExtensionUtil::RegisterFunction(db, ScalarFunction(
		"startValue", {SpatialSetType::geomset()},  
		SpatialSetType::WKB_BLOB(),    // return geometry as WKB --> it will be casted to geometry type in spatial
		SpatialSetFunctions::Set_start_value
	));
}

// startValue
void SpatialSetFunctions::Set_start_value(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	input.Flatten(args.size());
	auto input_data = FlatVector::GetData<string_t>(input);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		if (FlatVector::IsNull(input, i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		const string_t &blob = input_data[i];
		const uint8_t *data = (const uint8_t *)blob.GetData();
		size_t size = blob.GetSize();

		// Copy into MEOS Set*
		Set *s = (Set *)malloc(size);
		memcpy(s, data, size);

		Datum d = set_start_value(s);
        GSERIALIZED *g = DatumGetGserializedP(d);
             
        
        // Get size and data from the serialized geometry
        // size_t geo_size = g->size; 
        // const char* geo_data = reinterpret_cast<const char*>(g);            
        // string_t stored_result = StringVector::AddString(result, geo_data, geo_size); 
        // result_data[i] = stored_result;  

		size_t wkb_len = 0;
		uint8_t *wkb = geo_as_ewkb(g, "NDR", &wkb_len);
		
		string_t str = StringVector::AddString(result, std::string((char *)wkb, wkb_len));
		result_data[i] = str;

		// Clean up
		free(s);
		free(wkb);  
        
	}
}


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
