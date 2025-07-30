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
}

namespace duckdb {


// LogicalType SpanType::SPAN() {
//     auto type = LogicalType::STRUCT({
//         {"lower", LogicalType::BIGINT},
//         {"upper", LogicalType::BIGINT},
//         {"lower_inc",LogicalType::BOOLEAN},
//         {"upper_inc", LogicalType::BOOLEAN},
//         {"basetype", LogicalType::UTINYINT}
        
//     });
//     type.SetAlias("SPAN");
//     return type;
// }




LogicalType SpanType::SPAN() {
    LogicalType type(LogicalTypeId::VARCHAR);
    type.SetAlias("SPAN");
    return type;
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

inline void ExecuteDatespanToDatespan (DataChunk &args, ExpressionState &state, Vector &result) {
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

void SpanType::RegisterScalarFunctions(DatabaseInstance &instance) {
    
    // Function: string input -> canonicalized SPAN string output
    auto intspan_function = ScalarFunction(
        "INTSPAN",
        {LogicalType::VARCHAR},
        SpanType::SPAN(),  // Return SPAN (which is VARCHAR with alias)
        ExecuteSpanCreate
    );
    ExtensionUtil::RegisterFunction(instance, intspan_function);


    auto tstzspan_function = ScalarFunction(
        "tstzspan",
        {LogicalType::VARCHAR},
        SpanType::SPAN(),  // Return SPAN (which is VARCHAR with alias)
        ExecuteDatespanToDatespan
    );
    ExtensionUtil::RegisterFunction(instance, tstzspan_function);
}

void SpanType::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "SPAN", SpanType::SPAN());
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif