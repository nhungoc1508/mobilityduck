#define MOBILITYDUCK_EXTENSION_TYPES

#include "span.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include <regex>
#include <string>

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>
    #include <temporal/meos_catalog.h>
}

namespace duckdb {

#define DEFINE_SPAN_TYPE(NAME)                                        \
    LogicalType SpanTypes::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::VARCHAR);             \
        type.SetAlias(#NAME);                                        \
        return type;                                                 \
    }

DEFINE_SPAN_TYPE(INTSPAN)
DEFINE_SPAN_TYPE(BIGINTSPAN)
DEFINE_SPAN_TYPE(FLOATSPAN)
DEFINE_SPAN_TYPE(TEXTSPAN)
DEFINE_SPAN_TYPE(DATESPAN)
DEFINE_SPAN_TYPE(TSTZSPAN)

#undef DEFINE_SPAN_TYPE


void SpanTypes::RegisterTypes(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "INTSPAN", INTSPAN());
    ExtensionUtil::RegisterType(db, "BIGINTSPAN", BIGINTSPAN());
    ExtensionUtil::RegisterType(db, "FLOATSPAN", FLOATSPAN());
    ExtensionUtil::RegisterType(db, "TEXTSPAN", TEXTSPAN());
    ExtensionUtil::RegisterType(db, "DATESPAN", DATESPAN());
    ExtensionUtil::RegisterType(db, "TSTZSPAN", TSTZSPAN());    
}

const std::vector<LogicalType> &SpanTypes::AllTypes() {
    static std::vector<LogicalType> types = {
        INTSPAN(),
        BIGINTSPAN(),
        FLOATSPAN(),
        TEXTSPAN(),
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


inline void ExecuteSpanCreate(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];

    input_vec.Flatten(count);

    for (idx_t i = 0; i < count; i++) {
        std::string input = input_vec.GetValue(i).ToString();
        
        // Use span_in to parse and validate the input
        Span *span = span_in(input.c_str(), T_INTSPAN);
        
        if (span == NULL) {
            throw InvalidInputException("Invalid span format: " + input);
        }

        // Convert to canonicalized string using span_out
        char *str = span_out(span, 0);
        result.SetValue(i, Value(str));
        
        free(str);
        free(span);
    }

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

static void ExecuteSpanAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    auto meos_type = SpanTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());

    UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t input_str) -> string_t {
            Span *s = span_in(input_str.GetString().c_str(), meos_type);
            char *cstr = span_out(s, 15);
            string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        });
}

inline void ExecuteDatespanToTstzspan (DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vec = args.data[0];

    input_vec.Flatten(count);

    for (idx_t i = 0; i < count; i++) {
        std::string input = input_vec.GetValue(i).ToString();
        
        // Use span_in to parse and validate the input
        Span *span_val = span_in(input.c_str(), T_DATESPAN);
        Span *out_span = datespan_to_tstzspan(span_val);

        

        // Convert to canonicalized string using span_out
        char *str = span_out(out_span, 0);
        result.SetValue(i, Value(str));
        
        free(str);
        free(out_span);
    }

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void SpanTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    for (const auto &t : SpanTypes::AllTypes()){
        auto span_create_function = ScalarFunction(
            t.ToString(),
            {LogicalType::VARCHAR},
            t,
            ExecuteSpanCreate
        );
        ExtensionUtil::RegisterFunction(instance,span_create_function);
    }

 
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
        {LogicalType::VARCHAR},
        SpanTypes::TSTZSPAN(),  
        ExecuteDatespanToTstzspan
    );
    ExtensionUtil::RegisterFunction(instance, tstzspan_function);
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif