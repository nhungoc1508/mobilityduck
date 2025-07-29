#include "tgeometry.hpp"
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
    // #include <meos_internal_geo.h>
    // #include <geo/tgeo_spatialfuncs.h>
    // #include <geo/tspatial_parser.h>
    // #include <temporal/type_parser.h>
    #include <temporal/type_util.h>
    // #include <temporal/span.h>
}

TInstant **temparr_extract_duckdb(Value &array_value, int *count)
{
    // Get the list/array data
    auto &list_value = ListValue::GetChildren(array_value);
    *count = list_value.size();

    // Allocate memory for the result array
    TInstant **result = (TInstant**)malloc(sizeof(TInstant*) * (*count));
    
    // Extract each element from the list
    for (int i = 0; i < *count; i++) {
        auto element_value = list_value[i];
        
        if (element_value.IsNull()) {
            result[i] = nullptr;
        } else {
            // Convert the string to TInstant
            std::string tgeom_str = element_value.ToString();
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            result[i] = (TInstant*)temp;
        }
    }
    
    return result;
}


namespace duckdb {
    
    LogicalType TGeometryTypes::TGEOMETRY() {
        LogicalType type(LogicalTypeId::VARCHAR);
        type.SetAlias("TGEOMETRY");
        return type;
    }

    inline void ExecuteTGeometryFromString(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            
            Temporal *tinst = tgeometry_in(input.c_str());
            
            char *str = temporal_out(tinst, 0);

            result.SetValue(i, Value(str));

            free(str);
            free(tinst);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTgeometryFromTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
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
            if (!basetype_in(value.c_str(), temptype_basetype(T_TGEOMETRY), false, &base)) {
                continue; // Skip invalid geometry
            }

            const GSERIALIZED *gs = DatumGetGserializedP(PointerGetDatum(base));

            TInstant *inst = tgeoinst_make(gs, static_cast<TimestampTz>(t.value));

            if (inst) {
                char *str = tinstant_out(inst, 0);
                result.SetValue(i, Value(str));
                free(str);
                free(inst);
            }
            
            free(DatumGetPointer(base));
        }
    }
    
    inline void ExecuteTgeometryFromTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &value_vec = args.data[0];
        auto &span_vec = args.data[1];

        value_vec.Flatten(count);
        span_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            auto value = value_vec.GetValue(i).ToString();
            auto span_val = span_vec.GetValue(i).ToString();
            
            Datum base;
            if (!basetype_in(value.c_str(), temptype_basetype(T_TGEOMETRY), false, &base)) {
                continue; 
            }

            const GSERIALIZED *gs = DatumGetGserializedP(PointerGetDatum(base));
            const Span *span =span_in(span_val.c_str(), T_TSTZSPAN);


            TSequence *seq = tsequence_from_base_tstzspan(PointerGetDatum(gs), T_TGEOMETRY, span, LINEAR);

            
            char *str = tsequence_out(seq, 0);
            result.SetValue(i, Value(str));
            free(str);
            free(seq);
            
        }
    }

    inline void ExecuteTGeometryToTimeSpan(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            
            // Parse the input tgeompoint
            Temporal *temp = tgeometry_in(input.c_str());
            
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

    inline void ExecuteTGeometryToTnstant(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            Temporal *temp = tgeometry_in(input.c_str());

            TInstant *inst = temporal_to_tinstant(temp);

            char *str = tinstant_out(inst, 0);
            result.SetValue(i, Value(str));
            free(str);
            free(inst);
            free(temp);
        }
    }

    inline void ExecuteTGeometryToTimestamp(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            TInstant *temp = (TInstant *)tgeometry_in(input.c_str());

            TimestampTz meos_t = temp->t;

            // Set the POINT values
            timestamp_tz_t duckdb_t = static_cast<timestamp_tz_t>(meos_t + 946684800000000LL);

            result.SetValue(i, Value::TIMESTAMPTZ(duckdb_t));
            free(temp);
        }
    }

    inline void ExecuteTGeometryAsText(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];
        input_vec.Flatten(count);
        for(idx_t i = 0; i < count; i++){
            std::string input = input_vec.GetValue(i).ToString();
            Temporal *temp = tgeometry_in(input.c_str());
            char *str = tspatial_as_text(temp,0);
            result.SetValue(i, Value(str));
            free(str);
            free(temp);
        }
    }

    inline void ExecuteTGeometrySeqArr(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &tgeometry_arr_vec = args.data[0];    
        auto &interp_vec = args.data[1];
        auto &lower_vec = args.data[2];
        auto &upper_vec = args.data[3];
        
        tgeometry_arr_vec.Flatten(count);
        interp_vec.Flatten(count);
        lower_vec.Flatten(count);
        upper_vec.Flatten(count);
        
        for(idx_t i = 0; i < count; i++){
            auto array_value = tgeometry_arr_vec.GetValue(i);
            
            std::string interp_str = interp_vec.GetValue(i).ToString();
            interpType interp = interptype_from_string(interp_str.c_str());
            
            bool lower_inc = lower_vec.GetValue(i).GetValue<bool>();
            bool upper_inc = upper_vec.GetValue(i).GetValue<bool>();
            
            // Extract array elements using DuckDB's list functionality
            int element_count;
            TInstant **instants = temparr_extract_duckdb(array_value, &element_count);
            
            
            TSequence *sequence_result = tsequence_make((const TInstant **) instants, element_count, 
                                                    lower_inc, upper_inc, interp, NORMALIZE);
            
            if (sequence_result) {
                char *str = tsequence_out(sequence_result, 0);
                result.SetValue(i, Value(str));
                free(str);
                free(sequence_result);
            }
            
            // Clean up the instants array
            for (int j = 0; j < element_count; j++) {
                if (instants[j]) {
                    free(instants[j]);
                }
            }
            free(instants);
            
        }
        
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }


    inline void ExecuteTGeometrySeq(DataChunk &args, ExpressionState &state, Vector &result){
        char iterp_text =  'step';

        auto count = args.size();
        auto &tgeometry_vec = args.data[0];    
        auto &interp_vec = args.data[1];

        tgeometry_vec.Flatten(count);
        interp_vec.Flatten(count);
        

        for(idx_t i = 0; i < count; i++){
            std::string tgeometry_str = tgeometry_vec.GetValue(i).ToString();
            Temporal *temp = tgeometry_in(tgeometry_str.c_str());
            std::string interp_str = interp_vec.GetValue(i).ToString();
            interpType interp = interptype_from_string(interp_str.c_str());

            TSequence *seq = temporal_to_tsequence(temp, interp);

            char *str = tsequence_out(seq, 0);
            result.SetValue(i, Value(str));
            free(temp);
            free(str);
            free(seq);
            
        }     

    }

    inline void ExecuteTGeometryStartValue(DataChunk &args, ExpressionState &state, Vector &result){
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            Temporal *temp = tgeometry_in(input.c_str());

            Datum geo = temporal_start_value(temp);

            char *str = geo_as_text(DatumGetGserializedP(geo),0);

            // printf(str);

            result.SetValue(i, Value(str));
            free(str);
            free(temp);
        }
    }


    inline void ExecuteTGeometryAsEWKT(DataChunk &args, ExpressionState &state, Vector &result){

        auto count = args.size();
        auto tgeom_vec = args.data[0];
        tgeom_vec.Flatten(count);
        
        for (idx_t i =0; i < count; i++){
            std::string tgeometry_str = tgeom_vec.GetValue(i).ToString();
            Temporal * temp = tgeometry_in(tgeometry_str.c_str());
            char *ewkt = tspatial_as_ewkt(temp, 0);
            result.SetValue(i, Value(ewkt));
            free(ewkt);
            free(temp);
        }

    }


    void TGeometryTypes::RegisterScalarFunctions(DatabaseInstance &instance) {

        auto tgeometry_function = ScalarFunction(
            "TGEOMETRY", 
            {LogicalType::VARCHAR}, 
            TGeometryTypes::TGEOMETRY(), 
            ExecuteTGeometryFromString); 
        ExtensionUtil::RegisterFunction(instance, tgeometry_function);
        
        auto tgeometry_from_timestamp_function = ScalarFunction(
            "TGEOMETRY",
            {LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, 
            TGeometryTypes::TGEOMETRY(), 
            ExecuteTgeometryFromTimestamp);
        ExtensionUtil::RegisterFunction(instance, tgeometry_from_timestamp_function);

        auto tgeometry_from_tstzspan_function = ScalarFunction(
            "TGEOMETRY", // name
            {LogicalType::VARCHAR, SpanType::SPAN()}, 
            TGeometryTypes::TGEOMETRY(),  
            ExecuteTgeometryFromTstzspan);
        ExtensionUtil::RegisterFunction(instance, tgeometry_from_tstzspan_function);

        // Add the timeSpan function
        auto tgeometry_to_timespan_function = ScalarFunction(
            "timeSpan",
            {TGeometryTypes::TGEOMETRY()},     // takes a tgeompoint
            SpanType::SPAN(),               // returns your SPAN type
            ExecuteTGeometryToTimeSpan);
        ExtensionUtil::RegisterFunction(instance, tgeometry_to_timespan_function);

        auto tgeometry_to_tinstant_function = ScalarFunction(
            "tgeometryInst",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,  //*******************change to tinstant */
            ExecuteTGeometryToTnstant);
        ExtensionUtil::RegisterFunction(instance, tgeometry_to_tinstant_function);

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
        ExtensionUtil::RegisterFunction(instance, tgeometry_as_text_function);
        
        auto tgeometryseqarr_function = ScalarFunction(
            "tgeometrySeq", 
            {LogicalType::LIST(TGeometryTypes::TGEOMETRY()), LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
            TGeometryTypes::TGEOMETRY(),
            ExecuteTGeometrySeqArr
        );
        ExtensionUtil::RegisterFunction(instance, tgeometryseqarr_function);


        auto tgeometryseq_function = ScalarFunction(
            "tgeometrySeq", 
            {TGeometryTypes::TGEOMETRY(), LogicalType::VARCHAR},
            TGeometryTypes::TGEOMETRY(),
            ExecuteTGeometrySeq
        );
        ExtensionUtil::RegisterFunction(instance, tgeometryseq_function);

        auto tgeometry_start_value_function = ScalarFunction(
            "startValue", 
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,
            ExecuteTGeometryStartValue
        );
        ExtensionUtil::RegisterFunction(instance, tgeometry_start_value_function);

        auto TgeometryAsEWKT = ScalarFunction(
            "asEWKT",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,
            ExecuteTGeometryAsEWKT
        );
        ExtensionUtil::RegisterFunction(instance,TgeometryAsEWKT);

    }

    void TGeometryTypes::RegisterTypes(DatabaseInstance &instance) {
        ExtensionUtil::RegisterType(instance, "TGEOMETRY", TGeometryTypes::TGEOMETRY());
    }

}