#include "tgeometry.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include <regex>
#include <string>
#include <span.hpp>

extern "C" {
    #include <meos.h>
    #include <meos_geo.h>
    #include <meos_internal.h>
    #include <meos_internal_geo.h>
}

namespace duckdb {

LogicalType TGeometryTypes::TGEOMETRY() {
    auto type = LogicalType(LogicalTypeId::BLOB);
    type.SetAlias("TGEOMETRY");
    return type;
}

inline void ExecuteTGeometryFromString(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];
    
    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count, 
        [&](string_t input_geom_str) -> string_t {
            std::string input = input_geom_str.GetString();
            
            Temporal *tinst = tgeometry_in(input.c_str());
            if (!tinst) {
                throw InvalidInputException("Invalid TGEOMETRY input: " + input);
            }
            
            // Get the actual serialized size using MEOS function
            size_t data_size = temporal_mem_size(tinst);
            
            // Allocate buffer 
            uint8_t *data_buffer = (uint8_t*)malloc(data_size);
            if (!data_buffer) {
                free(tinst);  
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY data");
            }
            
            // Serialize the temporal object to binary format
            memcpy(data_buffer, tinst, data_size);
            
            // Create string_t from binary data and store it in the result vector
            string_t data_string_t(reinterpret_cast<const char*>(data_buffer), data_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, data_string_t);
            
            free(data_buffer);
            free(tinst);  
            
            return stored_data;
        });
    
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}



inline void ExecuteTgeometryFromTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &t_vec = args.data[1];

    BinaryExecutor::Execute<string_t, timestamp_tz_t, string_t>(
        value_vec, t_vec, result, count,
        [&](string_t value_str, timestamp_tz_t t) -> string_t {
            std::string value = value_str.GetString();
            
            // Use geom_in instead of basetype_in (from the available functions)
            GSERIALIZED *gs = geom_in(value.c_str(), -1); // -1 for no typmod constraint
            
            if (gs == NULL) {
                throw InvalidInputException("Invalid geometry format: " + value);
            }

            TInstant *inst = tgeoinst_make(gs, static_cast<TimestampTz>(t.value-946684800000000LL));

            if (inst == NULL) {
                free(gs);
                throw InvalidInputException("Failed to create TInstant");
            }

            size_t data_size = temporal_mem_size((Temporal*)inst);

            uint8_t *data_buffer = (uint8_t *)malloc(data_size);

            if (!data_buffer){
                free(inst);
                throw InvalidInputException("Failed to allocate memory to geometry data");
            }
            memcpy(data_buffer, inst, data_size);

            string_t data_string_t(reinterpret_cast<const char*>(data_buffer),data_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, data_string_t);
            
            free(data_buffer);
            free(inst);  
            
            return stored_data;

        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}



inline void ExecuteTGeometryAsEWKT(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> string_t {
            // Get the raw binary data
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_geom_str.GetData());
            size_t data_size = input_geom_str.GetSize();

            // Validate minimum size
            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            // CRITICAL FIX: Create a copy of the data first
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            // Cast to Temporal* - the data should be a valid MEOS temporal object
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            
            // Validate the temporal object
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }

            // Convert to EWKT using MEOS function
            char *ewkt = tspatial_as_ewkt(temp, 0);
            
            if (!ewkt) {
                free(data_copy);
                throw InvalidInputException("Failed to convert TGEOMETRY to EWKT");
            }
            
            // Create result string
            std::string result_str(ewkt);
            string_t stored_result = StringVector::AddString(result, result_str);
            
            // Clean up - free the EWKT string and our data copy
            free(ewkt);  // Use standard free for MEOS-allocated strings
            free(data_copy);
            
            return stored_result;
        }
    );

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TGeometryTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto tgeometry_function = ScalarFunction(
        "TGEOMETRY", 
        {LogicalType::VARCHAR}, 
        TGeometryTypes::TGEOMETRY(),
        ExecuteTGeometryFromString
    );
    ExtensionUtil::RegisterFunction(instance, tgeometry_function);
        
    auto tgeometry_from_timestamp_function = ScalarFunction(
        "TGEOMETRY",
        {LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, 
        TGeometryTypes::TGEOMETRY(), 
        ExecuteTgeometryFromTimestamp);
    ExtensionUtil::RegisterFunction(instance, tgeometry_from_timestamp_function);


    auto TgeometryAsEWKT = ScalarFunction(
        "asEWKT",
        {TGeometryTypes::TGEOMETRY()},
        LogicalType::VARCHAR,
        ExecuteTGeometryAsEWKT
    );
    ExtensionUtil::RegisterFunction(instance, TgeometryAsEWKT);
}

void TGeometryTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TGEOMETRY", TGeometryTypes::TGEOMETRY());
}

void TGeometryTypes::RegisterCastFunctions(DatabaseInstance &db) {
    // ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, TGeometryTypes::TGEOMETRY(), TgeometryFunctions::StringToTgeometry);
    // ExtensionUtil::RegisterCastFunction(db, TGeometryTypes::TGEOMETRY(), LogicalType::VARCHAR, TgeometryFunctions::TgeometryToString);
}

}