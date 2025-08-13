#include "tgeompoint.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include <regex>
#include <string>
#include <temporal/span.hpp>

extern "C" {
    #include <meos.h>
    #include <meos_geo.h>
    #include <meos_internal.h>
    #include <meos_internal_geo.h>
}

namespace duckdb {

LogicalType TGeomPointTypes::TGEOMPOINT() {
    auto type = LogicalType(LogicalTypeId::BLOB);
    type.SetAlias("TGEOMPOINT");
    return type;
}

LogicalType TGeomPointTypes::GEOMPOINT() {
    auto type = LogicalType(LogicalTypeId::BLOB);
    type.SetAlias("WKB_BLOB");
    return type;
}


inline void ExecuteTGeomPointFromString(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> string_t {
            std::string input = input_geom_str.GetString();
            
            // Create temporal geometry point from string input
            Temporal *tgeompoint = tgeompoint_in(input.c_str());
            if (!tgeompoint) {
                throw InvalidInputException("Invalid TGEOMPOINT input: " + input);
            }

            size_t wkb_size;
            // Convert to WKB format for consistent storage
            uint8_t *wkb_data = temporal_as_wkb(tgeompoint, 0, &wkb_size);
            
            if (wkb_data == NULL) {
                free(tgeompoint);
                throw InvalidInputException("Failed to convert TGEOMPOINT to WKB format");
            }

            // Create string_t from binary data
            string_t wkb_string_t(reinterpret_cast<const char*>(wkb_data), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);

            free(wkb_data);
            free(tgeompoint);
            
            return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointAsEWKT(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> string_t {
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_geom_str.GetData());
            size_t wkb_size = input_geom_str.GetSize();

            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);

            if (temp == NULL) {
                throw InvalidInputException("Invalid WKB data for TGEOMPOINT");
            }

            char *ewkt = tspatial_as_ewkt(temp, 0);
            
            if (ewkt == NULL) {
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMPOINT to text");
            }
            
            std::string result_str(ewkt);
            free(ewkt);
            free(temp);
            
            return StringVector::AddString(result, result_str);
        }
    );

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TgeomPointFunctions::StringToTgeomPoint(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_geom_str) -> string_t {
            std::string input_str = input_geom_str.GetString();

            // Create temporal geometry point from string
            Temporal *t = tgeompoint_in(input_str.c_str());
            if (!t) {
                throw InvalidInputException("Invalid TGEOMPOINT input: " + input_str);
            }

            size_t wkb_size;
            // Use consistent WKB conversion
            uint8_t *wkb = temporal_as_wkb(t, 0, &wkb_size);
            if (!wkb) {
                free(t);
                throw InvalidInputException("Failed to convert to WKB");
            }

            string_t wkb_string(reinterpret_cast<const char *>(wkb), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string);

            free(wkb);
            free(t);
            
            return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
    return true;
}

bool TgeomPointFunctions::TgeomPointToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_blob) -> string_t {
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t *>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();

            Temporal *t = temporal_from_wkb(wkb_data, wkb_size);
            if (!t) {
                throw InvalidInputException("Invalid WKB data for TGEOMPOINT");
            }

            char *cstr = temporal_out(t, 15);
            std::string output(cstr);

            free(t);
            free(cstr);
            return StringVector::AddString(result, output);
        });
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
    return true;   
}

inline void ExecuteTGeomPointStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];
    
    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, count,
        [&](string_t input_blob) -> string_t {
            // Extract WKB data from the input blob (same as other functions)
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();

            // Reconstruct temporal object from WKB
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (!temp) {
                throw InvalidInputException("Invalid WKB data for TGEOMPOINT");
            }

            // Get the start value as a Datum, then convert to GSERIALIZED*
            Datum start_datum = temporal_start_value(temp);
            GSERIALIZED *start_geom = DatumGetGserializedP(start_datum);
            if (!start_geom) {
                free(temp);
                throw InvalidInputException("Failed to get start value from TGEOMPOINT");
            }

            // Convert geometry to EWKB format for consistent storage
            size_t ewkb_size;
            uint8_t *ewkb_data = geo_as_ewkb(start_geom, NULL, &ewkb_size);
            if (!ewkb_data) {
                free(temp);
                throw InvalidInputException("Failed to convert start geometry to EWKB");
            }

            // Create string_t from EWKB data
            string_t ewkb_string(reinterpret_cast<const char*>(ewkb_data), ewkb_size);
            string_t stored_result = StringVector::AddStringOrBlob(result, ewkb_string);

            // Clean up memory
            free(ewkb_data);
            free(temp);
            
            return stored_result;
        });
    
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TGeomPointTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    // Main TGEOMPOINT constructor function
    auto tgeompoint_function = ScalarFunction(
        "TGEOMPOINT", 
        {LogicalType::VARCHAR}, 
        TGeomPointTypes::TGEOMPOINT(),
        ExecuteTGeomPointFromString
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_function);

    auto TgeompointAsEWKT = ScalarFunction(
        "asEWKT",
        {TGeomPointTypes::TGEOMPOINT()},
        LogicalType::VARCHAR,
        ExecuteTGeomPointAsEWKT
    );
    ExtensionUtil::RegisterFunction(instance, TgeompointAsEWKT);

    auto tgeometry_start_value_function = ScalarFunction(
        "startValue", 
        {TGeomPointTypes::TGEOMPOINT()},
        TGeomPointTypes::GEOMPOINT(),
        ExecuteTGeomPointStartValue
    );
    ExtensionUtil::RegisterFunction(instance, tgeometry_start_value_function);
}

void TGeomPointTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TGEOMPOINT", TGeomPointTypes::TGEOMPOINT());
}

void TGeomPointTypes::RegisterCastFunctions(DatabaseInstance &db) {
    ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, TGeomPointTypes::TGEOMPOINT(), TgeomPointFunctions::StringToTgeomPoint);
    ExtensionUtil::RegisterCastFunction(db, TGeomPointTypes::TGEOMPOINT(), LogicalType::VARCHAR, TgeomPointFunctions::TgeomPointToString);
}

}