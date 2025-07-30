#include "tgeompoint.hpp"
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
    #include <meos_internal_geo.h>
    #include <geo/tgeo_spatialfuncs.h>
    #include <geo/tspatial_parser.h>
    #include <temporal/type_parser.h>
    #include <temporal/type_util.h>
    #include <temporal/span.h>
}


namespace duckdb {
    
    LogicalType PointTypes::TGEOMPOINT() {
        LogicalType type(LogicalTypeId::VARCHAR);
        type.SetAlias("TGEOMPOINT");
        return type;
    }
    
    inline void ExecuteTGeomPointFromString(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            
            Temporal *tinst = tgeompoint_in(input.c_str());
            
            char *str = temporal_out(tinst, 0);

            result.SetValue(i, Value(str));

            free(str);
            free(tinst);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTimeSpan(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            
            // Parse the input tgeompoint
            Temporal *temp = tgeompoint_in(input.c_str());
            
            if (temp == NULL) {
                throw InvalidInputException("Invalid tgeompoint format: " + input);
            }
            
            // Extract time span using MEOS function
            Span *timespan = temporal_to_tstzspan(temp);
            
            if (timespan == NULL) {
                free(temp);
                throw InvalidInputException("Failed to extract timespan from tgeompoint: " + input);
            }
            
            // Convert to string using span_out
            char *str = span_out(timespan, 0);
            result.SetValue(i, Value(str));
            
            free(str);
            free(timespan);
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTemporalToTnstant(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            Temporal *temp = tgeompoint_in(input.c_str());

            TInstant *inst = temporal_to_tinstant(temp);

            char *str = tinstant_out(inst, 0);
            result.SetValue(i, Value(str));
            free(str);
            free(inst);
            free(temp);
        }
    }

    inline void ExecuteTemporalToTimestamp(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            TInstant *temp = (TInstant *)tgeompoint_in(input.c_str());

            TimestampTz meos_t = temp->t;

            // Set the POINT values
            timestamp_tz_t duckdb_t = static_cast<timestamp_tz_t>(meos_t + 946684800000000LL);

            result.SetValue(i, Value::TIMESTAMPTZ(duckdb_t));
            free(temp);
        }
    }


    inline void ExecuteTemporalAsText(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];
        input_vec.Flatten(count);
        for(idx_t i = 0; i < count; i++){
            std::string input = input_vec.GetValue(i).ToString();
            Temporal *temp = tgeompoint_in(input.c_str());
            char *str = tspatial_as_text(temp,0);
            result.SetValue(i, Value(str));
            free(str);
            free(temp);
        }
    }
    
    inline void ExecuteTgeompointFromTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &value_vec = args.data[0];
        auto &t_vec = args.data[1];

        value_vec.Flatten(count);
        t_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            auto t = t_vec.GetValue(i).GetValue<timestamp_tz_t>();
            auto value = value_vec.GetValue(i).ToString();
            
            
            // Option 1: Use basetype_in directly (bypasses delimiter requirement)
            Datum base;
            if (!basetype_in(value.c_str(), temptype_basetype(T_TGEOMPOINT), false, &base)) {
                continue; // Skip invalid geometry
            }

            const GSERIALIZED *gs = DatumGetGserializedP(PointerGetDatum(base));

            TInstant *inst = tpointinst_make(gs, static_cast<TimestampTz>(t.value));

            // TInstant *inst = tinstant_make(base, T_TGEOMPOINT, (TimestampTz)t.value);

            if (inst) {
                char *str = tinstant_out(inst, 0);
                result.SetValue(i, Value(str));
                free(str);
                free(inst);
            }
            
            free(DatumGetPointer(base));
        }
    }


    

    inline void ExecuteTgeompointFromTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &value_vec = args.data[0];
        auto &span_vec = args.data[1];

        value_vec.Flatten(count);
        span_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            auto value = value_vec.GetValue(i).ToString();
            auto span_val = span_vec.GetValue(i).ToString();
            
            Datum base;
            if (!basetype_in(value.c_str(), temptype_basetype(T_TGEOMPOINT), false, &base)) {
                continue; 
            }

            // Datum tstzspan;
            // if (!basetype_in(span_val.c_str(), temptype_basetype(T_DATE), false, &tstzspan)){
            //     continue;
            // }

            const GSERIALIZED *gs = DatumGetGserializedP(PointerGetDatum(base));
            const Span *span =span_in(span_val.c_str(), T_TSTZSPAN);


            TSequence *seq = tsequence_from_base_tstzspan(PointerGetDatum(gs), T_TGEOMPOINT, span, LINEAR);

            
            char *str = tsequence_out(seq, 0);
            result.SetValue(i, Value(str));
            free(str);
            free(seq);
            
        }
    }


    void PointTypes::RegisterScalarFunctions(DatabaseInstance &instance) {

        auto tgeompoint_function = ScalarFunction(
            "TGEOMPOINT", // name
            {LogicalType::VARCHAR}, // string input
            PointTypes::TGEOMPOINT(), // return TGEOMPOINT struct
            ExecuteTGeomPointFromString); // function
        ExtensionUtil::RegisterFunction(instance, tgeompoint_function);


        // Add the timeSpan function
        auto timespan_function = ScalarFunction(
            "timeSpan",
            {PointTypes::TGEOMPOINT()},     // takes a tgeompoint
            SpanType::SPAN(),               // returns your SPAN type
            ExecuteTimeSpan);
        ExtensionUtil::RegisterFunction(instance, timespan_function);

        auto tinstant_function = ScalarFunction(
            "tgeompointInst",
            {PointTypes::TGEOMPOINT()},
            LogicalType::VARCHAR,  //*******************change to tinstant */
            ExecuteTemporalToTnstant);
        ExtensionUtil::RegisterFunction(instance, tinstant_function);

        auto gettimespan_function = ScalarFunction(
            "getTimestamp",
            {PointTypes::TGEOMPOINT()},
            LogicalType::TIMESTAMP_TZ,  //*******************change to tinstant */
            ExecuteTemporalToTimestamp);
        ExtensionUtil::RegisterFunction(instance, gettimespan_function);

        auto as_text_function = ScalarFunction(
            "asText", 
            {PointTypes::TGEOMPOINT()},
            LogicalType::VARCHAR,
            ExecuteTemporalAsText
        );
        ExtensionUtil::RegisterFunction(instance, as_text_function);

        auto tgeompoint_timestamp_function = ScalarFunction(
            "TGEOMPOINT", // name
            {LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, // inputs
            PointTypes::TGEOMPOINT(), 
            ExecuteTgeompointFromTimestamp);
        ExtensionUtil::RegisterFunction(instance, tgeompoint_timestamp_function);

        auto tgeompoint_tstzspan_function = ScalarFunction(
            "TGEOMPOINT", // name
            {LogicalType::VARCHAR, SpanType::SPAN()}, // inputs
            PointTypes::TGEOMPOINT(), 
            ExecuteTgeompointFromTstzspan);
        ExtensionUtil::RegisterFunction(instance, tgeompoint_tstzspan_function);

    }

    void PointTypes::RegisterTypes(DatabaseInstance &instance) {
        ExtensionUtil::RegisterType(instance, "TGEOMPOINT", PointTypes::TGEOMPOINT());
    }

}