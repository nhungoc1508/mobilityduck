#define MOBILITYDUCK_EXTENSION_TYPES

#include "span.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include <regex>
#include <string>

extern "C" {
    // #include <postgres.h>
    // #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_geo.h>
    #include <meos_internal.h>
    // #include <temporal/meos_catalog.h>
}


namespace duckdb {

#define DEFINE_SPAN_TYPE(NAME)                                        \
    LogicalType SpanTypes::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::BLOB);                \
        type.SetAlias(#NAME);                                        \
        return type;                                                 \
    }

DEFINE_SPAN_TYPE(INTSPAN)
DEFINE_SPAN_TYPE(BIGINTSPAN)
DEFINE_SPAN_TYPE(FLOATSPAN)
DEFINE_SPAN_TYPE(DATESPAN)
DEFINE_SPAN_TYPE(TSTZSPAN)

#undef DEFINE_SPAN_TYPE

void SpanTypes::RegisterTypes(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "INTSPAN", INTSPAN());
    ExtensionUtil::RegisterType(db, "BIGINTSPAN", BIGINTSPAN());
    ExtensionUtil::RegisterType(db, "FLOATSPAN", FLOATSPAN());
    ExtensionUtil::RegisterType(db, "DATESPAN", DATESPAN());
    ExtensionUtil::RegisterType(db, "TSTZSPAN", TSTZSPAN());    
}

const std::vector<LogicalType> &SpanTypes::AllTypes() {
    static std::vector<LogicalType> types = {
        INTSPAN(),
        BIGINTSPAN(),
        FLOATSPAN(),
        DATESPAN(),
        TSTZSPAN()
    };
    return types;
}

meosType SpanTypeMapping::GetMeosTypeFromAlias(const std::string &alias) {
    static const std::unordered_map<std::string, meosType> alias_to_type = {
        {"INTSPAN", T_INTSPAN},
        {"BIGINTSPAN", T_BIGINTSPAN},
        {"FLOATSPAN", T_FLOATSPAN},
        {"DATESPAN", T_DATESPAN},
        {"TSTZSPAN", T_TSTZSPAN}        
    };

    auto it = alias_to_type.find(alias);
    if (it != alias_to_type.end()) {
        return it->second;
    } else {
        return T_UNKNOWN;
    }
}

// Refactored to use UnaryExecutor
inline void ExecuteSpanCreate(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vec = args.data[0];
    auto &result_type = result.GetType();
    std::string type_alias = result_type.GetAlias();
    
    // Map the alias to the correct MEOS type
    meosType target_meos_type = SpanTypeMapping::GetMeosTypeFromAlias(type_alias);
    
    if (target_meos_type == T_UNKNOWN) {
        throw InvalidInputException("Unknown span type: " + type_alias);
    }

    UnaryExecutor::Execute<string_t, string_t>(
        input_vec, result, args.size(),
        [&](string_t input_str) -> string_t {
            std::string input = input_str.GetString();
            
            // Use the correct MEOS type for parsing
            Span *span = span_in(input.c_str(), target_meos_type);
            
            if (span == NULL) {
                throw InvalidInputException("Invalid " + type_alias + " format: " + input);
            }

            // Convert to WKB format
            size_t wkb_size;
            uint8_t *wkb_data = span_as_wkb(span, WKB_EXTENDED, &wkb_size);
            
            if (wkb_data == NULL) {
                free(span);
                throw InvalidInputException("Failed to convert span to WKB format");
            }

            // Create string_t from binary data and add to result vector
            string_t wkb_string_t(reinterpret_cast<const char*>(wkb_data), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            free(wkb_data);
            free(span);
            
            return stored_data;
        });
}

// Already using UnaryExecutor - no changes needed
inline void ExecuteSpanAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> string_t {
            // Convert binary string_t back to span
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Span *s = span_from_wkb(wkb_data, wkb_size);
            if (s == NULL) {
                throw InvalidInputException("Invalid WKB data for span");
            }
            
            char *cstr = span_out(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            
            return StringVector::AddString(result, output);
        });
}

// Already using UnaryExecutor - no changes needed
inline void ExecuteDatespanToTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> string_t {
            // Convert binary string_t back to span
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Span *input_span = span_from_wkb(wkb_data, wkb_size);
            if (input_span == NULL) {
                throw InvalidInputException("Invalid WKB data for DATESPAN");
            }
            
            Span *output_span = datespan_to_tstzspan(input_span);
            if (output_span == NULL) {
                free(input_span);
                throw InvalidInputException("Failed to convert DATESPAN to TSTZSPAN");
            }
            
            size_t output_wkb_size;
            uint8_t *output_wkb_data = span_as_wkb(output_span, WKB_EXTENDED, &output_wkb_size);
            if (output_wkb_data == NULL) {
                free(input_span);
                free(output_span);
                throw InvalidInputException("Failed to convert TSTZSPAN to WKB format");
            }

            // Create the result string_t
            string_t wkb_string_t(reinterpret_cast<const char*>(output_wkb_data), output_wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            // Clean up memory
            free(output_wkb_data);
            free(output_span);
            free(input_span);
            
            return stored_data;
        });
}


// Refactored to use UnaryExecutor
bool SpanFunctions::StringToSpan(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    // Get the target type from the result vector
    auto &result_type = result.GetType();
    std::string type_alias = result_type.GetAlias();
    
    // Map the alias to the correct MEOS type
    meosType target_meos_type = SpanTypeMapping::GetMeosTypeFromAlias(type_alias);
    
    if (target_meos_type == T_UNKNOWN) {
        throw InvalidInputException("Unknown span type: " + type_alias);
    }

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_str) -> string_t {
            std::string input = input_str.GetString();
            
            // Use the correct MEOS type for parsing
            Span *span = span_in(input.c_str(), target_meos_type);
            
            if (span == NULL) {
                throw InvalidInputException("Invalid " + type_alias + " format: " + input);
            }

            // Convert to WKB format
            size_t wkb_size;
            uint8_t *wkb_data = span_as_wkb(span, WKB_EXTENDED, &wkb_size);
            
            if (wkb_data == NULL) {
                free(span);
                throw InvalidInputException("Failed to convert span to WKB format");
            }

            // Create string_t from binary data and add to result vector
            string_t wkb_string_t(reinterpret_cast<const char*>(wkb_data), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            free(wkb_data);
            free(span);
            
            return stored_data;
        });

    return true;
}

// Already using UnaryExecutor - removed redundant vector type setting
bool SpanFunctions::SpanToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_blob) -> string_t {
            // Convert binary string_t back to span
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            
            Span *s = span_from_wkb(wkb_data, wkb_size);
            if (s == NULL) {
                throw InvalidInputException("Invalid WKB data for span");
            }
            
            char *cstr = span_out(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            
            return StringVector::AddString(result, output);
        });

    return true;
}

void SpanTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    // Register span creation functions (from text to BLOB)
    for (const auto &t : SpanTypes::AllTypes()){
        auto span_create_function = ScalarFunction(
            t.ToString(),
            {LogicalType::VARCHAR},
            t,
            ExecuteSpanCreate
        );
        ExtensionUtil::RegisterFunction(instance, span_create_function);
    }

    // Register asText functions (from BLOB to text)
    for (const auto &t : SpanTypes::AllTypes()) {
        auto span_as_text = ScalarFunction(
            "asText", 
            {t},
            LogicalType::VARCHAR, 
            ExecuteSpanAsText
        );
        ExtensionUtil::RegisterFunction(instance, span_as_text);
    }

    auto tstzspan_function = ScalarFunction(
        "tstzspan",
        {SpanTypes::DATESPAN()},
        SpanTypes::TSTZSPAN(),  
        ExecuteDatespanToTstzspan
    );
    ExtensionUtil::RegisterFunction(instance, tstzspan_function);

    auto tstzspan_from_string_function = ScalarFunction(
        "tstzspan",
        {LogicalType::VARCHAR},
        SpanTypes::TSTZSPAN(),
        ExecuteSpanCreate  // Reuse the same logic as other span creation functions
    );
    ExtensionUtil::RegisterFunction(instance, tstzspan_from_string_function);
}

void SpanTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    // Register cast functions from VARCHAR to each span type
    for (const auto &t : SpanTypes::AllTypes()) {
        ExtensionUtil::RegisterCastFunction(
            instance,
            LogicalType::VARCHAR,
            t,
            SpanFunctions::StringToSpan
        );

        ExtensionUtil::RegisterCastFunction(
            instance,
            t,
            LogicalType::VARCHAR,
            SpanFunctions::SpanToString
        );
    }
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif