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
    auto &input_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, count,
        [&](string_t input_string) -> string_t {
            std::string input = input_string.GetString();
            
            // Add proper type parameter (adjust T_TGEOMPOINT as needed)
            Temporal *tinst = tgeometry_in(input.c_str());
            if (!tinst) {
                throw InvalidInputException("Invalid TGEOMETRY input: " + input);
            }

            size_t wkb_size;
            // Use consistent WKB conversion
            uint8_t *wkb_data = temporal_as_wkb(tinst, 0, &wkb_size);
            
            if (wkb_data == NULL) {
                free(tinst);
                throw InvalidInputException("Failed to convert TGEOMETRY to WKB format");
            }

            // Create string_t from binary data
            string_t wkb_string_t(reinterpret_cast<const char*>(wkb_data), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);

            free(wkb_data);
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

            // Convert TInstant to WKB binary data (same as ExecuteTGeometryFromString)
            size_t wkb_size;
            uint8_t *wkb_data = temporal_as_wkb(reinterpret_cast<Temporal*>(inst), 0, &wkb_size);
            
            if (wkb_data == NULL) {
                free(inst);
                free(gs);
                throw InvalidInputException("Failed to convert TInstant to WKB format");
            }

            // Create string_t from binary data and store as BLOB
            string_t wkb_string_t(reinterpret_cast<const char*>(wkb_data), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            // Clean up memory
            free(wkb_data);
            free(inst);
            free(gs);
            
            return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}



inline void ExecuteTgeometryFromTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &span_vec = args.data[1];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        value_vec, span_vec, result, count,
        [&](string_t value_str, string_t span_blob) -> string_t {
            std::string value = value_str.GetString();
            
            // Use geom_in for geometry parsing
            GSERIALIZED *gs = geom_in(value.c_str(), -1);
            if (gs == NULL) {
                throw InvalidInputException("Invalid geometry format: " + value);
            }

            // Convert the TSTZSPAN blob back to a MEOS Span object
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(span_blob.GetData());
            size_t wkb_size = span_blob.GetSize();
            
            const Span *span = span_from_wkb(wkb_data, wkb_size);
            if (span == NULL) {
                free(gs);
                throw InvalidInputException("Invalid WKB data for TSTZSPAN");
            }

            TSequence *seq = tsequence_from_base_tstzspan(Datum(gs), T_TGEOMETRY, span, STEP);
            if (seq == NULL) {
                free(gs);
                free((void*)span);
                throw InvalidInputException("Failed to create TSequence");
            }

            // Convert to WKB binary format for consistent storage
            size_t output_wkb_size;
            uint8_t *output_wkb_data = temporal_as_wkb(reinterpret_cast<Temporal*>(seq), 0, &output_wkb_size);
            
            if (output_wkb_data == NULL) {
                free(seq);
                free(gs);
                free((void*)span);
                throw InvalidInputException("Failed to convert to WKB format");
            }

            // Create string_t from binary data and store as BLOB
            string_t wkb_string_t(reinterpret_cast<const char*>(output_wkb_data), output_wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            // Clean up memory
            free(output_wkb_data);
            free(seq);
            free(gs);
            free((void*)span);
            
            return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}


 inline void ExecuteTGeometryToTimeSpan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, count,
        [&](string_t input_blob) -> string_t {
            // Convert WKB blob back to Temporal object
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (temp == NULL) {
                throw InvalidInputException("Invalid WKB data for TGEOMETRY");
            }
            
            // Extract time span using MEOS function
            Span *timespan = temporal_to_tstzspan(temp);
            if (timespan == NULL) {
                free(temp);
                throw InvalidInputException("Failed to extract timespan from TGEOMETRY");
            }
            
            // Convert span to WKB binary data for consistent storage
            size_t span_wkb_size;
            uint8_t *span_wkb_data = span_as_wkb(timespan, 0, &span_wkb_size);
            if (span_wkb_data == NULL) {
                free(timespan);
                free(temp);
                throw InvalidInputException("Failed to convert timespan to WKB format");
            }
            
            // Create string_t from binary data and store as BLOB
            string_t wkb_string_t(reinterpret_cast<const char*>(span_wkb_data), span_wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            free(span_wkb_data);
            free(timespan);
            free(temp);
            
            return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}


inline void ExecuteTGeometryToTInstant(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, count,
        [&](string_t input_blob) -> string_t {
            // Convert WKB blob back to Temporal object
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (temp == NULL) {
                printf("%s", 'Invalid WKB data for TGEOMETRY');
                throw InvalidInputException("Invalid WKB data for TGEOMETRY");
            }
            
            // Convert to TInstant
            TInstant *inst = temporal_to_tinstant(temp);
            if (inst == NULL) {
                printf("%s", 'Failed to convert TGEOMETRY to TInstant');
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMETRY to TInstant");
            }
            
            // Convert TInstant to WKB binary data for consistent storage
            size_t inst_wkb_size;
            uint8_t *inst_wkb_data = temporal_as_wkb(reinterpret_cast<Temporal*>(inst), 0, &inst_wkb_size);
            if (inst_wkb_data == NULL) {
                printf("%s", 'Failed to convert TInstant to WKB format');
                free(inst);
                free(temp);
                throw InvalidInputException("Failed to convert TInstant to WKB format");
            }
            
            // // Create string_t from binary data and store as BLOB
            string_t wkb_string_t(reinterpret_cast<const char*>(inst_wkb_data), inst_wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            free(inst_wkb_data);
            free(inst);
            free(temp);
            
            // return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeometryToTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];

    UnaryExecutor::Execute<string_t, timestamp_tz_t>(
        input_vec, result, count,
        [&](string_t input_blob) -> timestamp_tz_t {
            // Convert WKB blob back to Temporal object
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (temp == NULL) {
                throw InvalidInputException("Invalid WKB data for TGEOMETRY");
            }
            
            // Convert to TInstant (works for any temporal type)
            TInstant *tinst = temporal_to_tinstant(temp);
            if (tinst == NULL) {
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMETRY to TInstant");
            }
            
            // Extract timestamp
            TimestampTz meos_t = tinst->t;
            
            // Convert MEOS timestamp to DuckDB timestamp
            timestamp_tz_t duckdb_t = static_cast<timestamp_tz_t>(meos_t + 946684800000000LL);
            
            free(tinst);
            free(temp);
            return duckdb_t;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeometryAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, count,
        [&](string_t input_blob) -> string_t {
            // Convert WKB blob back to Temporal object
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (temp == NULL) {
                throw InvalidInputException("Invalid WKB data for TGEOMETRY");
            }
            
            // Convert to text representation
            char *str = tspatial_as_text(temp, 0);
            if (str == NULL) {
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMETRY to text");
            }
            
            std::string result_str(str);
            free(str);
            free(temp);
            
            return StringVector::AddString(result, result_str);
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

    

bool TgeometryFunctions::StringToTgeometry(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_string) -> string_t {
            std::string input_str = input_string.GetString();

            // Add type parameter
            Temporal *t = tgeometry_in(input_str.c_str());
            if (!t) {
                throw InvalidInputException("Invalid TGEOMETRY input: " + input_str);
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

bool TgeometryFunctions::TgeometryToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_blob) -> string_t {
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t *>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();

            Temporal *t = temporal_from_wkb(wkb_data, wkb_size);
            if (!t) {
                throw InvalidInputException("Invalid WKB data for TGEOMETRY");
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

    auto tgeometry_from_tstzspan_function = ScalarFunction(
        "TGEOMETRY", // name
        {LogicalType::VARCHAR, SpanTypes::TSTZSPAN()}, 
        TGeometryTypes::TGEOMETRY(),  
        ExecuteTgeometryFromTstzspan);
    ExtensionUtil::RegisterFunction(instance, tgeometry_from_tstzspan_function);

        auto tgeometry_to_timespan_function = ScalarFunction(
        "timeSpan",
        {TGeometryTypes::TGEOMETRY()},
        SpanTypes::TSTZSPAN(),
        ExecuteTGeometryToTimeSpan
    );
    ExtensionUtil::RegisterFunction(instance, tgeometry_to_timespan_function);

    // auto tgeometry_to_tinstant_function = ScalarFunction(
    //     "tgeometryInst",
    //     {TGeometryTypes::TGEOMETRY()},
    //     LogicalType::VARCHAR,
    //     ExecuteTGeometryToTInstant
    // );
    // ExtensionUtil::RegisterFunction(instance, tgeometry_to_tinstant_function);

    auto tgeometry_gettimespan_function = ScalarFunction(
        "getTimestamp",
        {TGeometryTypes::TGEOMETRY()},
        LogicalType::TIMESTAMP_TZ,  //*******************change to tinstant */
        ExecuteTGeometryToTimestamp);
    ExtensionUtil::RegisterFunction(instance, tgeometry_gettimespan_function);

    auto tgeometry_as_text_function = ScalarFunction(
        "asText", 
        {TGeometryTypes::TGEOMETRY()},
        LogicalType::VARCHAR,
        ExecuteTGeometryAsText
    );
    
}

void TGeometryTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TGEOMETRY", TGeometryTypes::TGEOMETRY());
}

void TGeometryTypes::RegisterCastFunctions(DatabaseInstance &db) {
    ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, TGeometryTypes::TGEOMETRY(), TgeometryFunctions::StringToTgeometry);
    ExtensionUtil::RegisterCastFunction(db, TGeometryTypes::TGEOMETRY(), LogicalType::VARCHAR, TgeometryFunctions::TgeometryToString);
}
}
