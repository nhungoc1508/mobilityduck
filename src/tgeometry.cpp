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
        const char* default_iterp =  "step";

        auto count = args.size();
        auto &tgeometry_vec = args.data[0];  
        
        tgeometry_vec.Flatten(count);
        

        Vector *interp_vec = nullptr;
        if (args.data.size() > 1){
            interp_vec = &args.data[1];
            interp_vec->Flatten(count);
        }  

        for(idx_t i = 0; i < count; i++){
            std::string tgeometry_str = tgeometry_vec.GetValue(i).ToString();
            Temporal *temp = tgeometry_in(tgeometry_str.c_str());
            std::string interp_str;

            if (interp_vec && !interp_vec->GetValue(i).IsNull()){
                interp_str = interp_vec->GetValue(i).ToString();
            }
            else{
                interp_str = default_iterp;
            }
            
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


    inline void ExecuteTGeometrySetInterp(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];
        auto &interp_vec = args.data[1];

        tgeom_vec.Flatten(count);
        interp_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            std::string interp_str = interp_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            interpType new_interp = interptype_from_string(interp_str.c_str());
            
            Temporal *result_temp = temporal_set_interp(temp, new_interp);
            
            char *str = temporal_out(result_temp, 0);
            result.SetValue(i, Value(str));
            
            free(str);
            free(temp);
            free(result_temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }



    inline void ExecuteTGeometryMerge(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom1_vec = args.data[0];
        auto &tgeom2_vec = args.data[1];

        tgeom1_vec.Flatten(count);
        tgeom2_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom1_str = tgeom1_vec.GetValue(i).ToString();
            std::string tgeom2_str = tgeom2_vec.GetValue(i).ToString();
            
            Temporal *temp1 = tgeometry_in(tgeom1_str.c_str());
            Temporal *temp2 = tgeometry_in(tgeom2_str.c_str());
            
            Temporal *result_temp = temporal_merge(temp1, temp2);
            
            char *str = temporal_out(result_temp, 0);
            result.SetValue(i, Value(str));
            
            free(str);
            free(temp1);
            free(temp2);
            free(result_temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryTempSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];

        tgeom_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            const char *subtype_str = temporal_subtype(temp);

            text *subtype = cstring2text(subtype_str);
            
            result.SetValue(i, Value(subtype));
            
            free(subtype);
            // free(subtype_str);
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryInterp(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];

        tgeom_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            const char *interp_str = temporal_interp(temp);

            text *interp = cstring2text(interp_str);
            
            result.SetValue(i, Value(interp));
            
            // free(interp_str);
            free(interp);
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];

        tgeom_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            size_t mem_size = temporal_mem_size(temp);
            
            result.SetValue(i, Value::INTEGER(static_cast<int32_t>(mem_size)));
            
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }


    inline void ExecuteTGeometryGetValue(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            TInstant *tinst = (TInstant *)tgeometry_in(input.c_str());

            Datum geo = tinstant_value(tinst);

            char *str = geo_as_text(DatumGetGserializedP(geo), 0);

            result.SetValue(i, Value(str));
            free(str);
            free(tinst);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryValueN(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];
        auto &n_vec = args.data[1];

        tgeom_vec.Flatten(count);
        n_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            int32_t n = n_vec.GetValue(i).GetValue<int32_t>();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            Datum geo_result;
            bool found = temporal_value_n(temp, n, &geo_result);
            
            if (!found) {
                result.SetValue(i, Value());  // Set NULL value
            } else {
                char *str = geo_as_text(DatumGetGserializedP(geo_result), 0);
                result.SetValue(i, Value(str));
                free(str);
            }
            
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &input_vec = args.data[0];

        input_vec.Flatten(count);
        for (idx_t i = 0; i < count; i++) {
            std::string input = input_vec.GetValue(i).ToString();
            Temporal *temp = tgeometry_in(input.c_str());

            Datum geo = temporal_end_value(temp);

            char *str = geo_as_text(DatumGetGserializedP(geo), 0);

            result.SetValue(i, Value(str));
            free(str);
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryLowerInc(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];

        tgeom_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            bool lower_inc = temporal_lower_inc(temp);
            
            result.SetValue(i, Value::BOOLEAN(lower_inc));
            
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryUpperInc(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];

        tgeom_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            bool upper_inc = temporal_upper_inc(temp);
            
            result.SetValue(i, Value::BOOLEAN(upper_inc));
            
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryStartInstant(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];

        tgeom_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            TInstant *start_inst = temporal_start_instant(temp);
            
            char *str = tinstant_out(start_inst, 0);
            
            result.SetValue(i, Value(str));
            
            free(str);
            free(start_inst);
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    inline void ExecuteTGeometryInstantN(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tgeom_vec = args.data[0];
        auto &n_vec = args.data[1];

        tgeom_vec.Flatten(count);
        n_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            std::string tgeom_str = tgeom_vec.GetValue(i).ToString();
            int32_t n = n_vec.GetValue(i).GetValue<int32_t>();
            
            Temporal *temp = tgeometry_in(tgeom_str.c_str());
            
            TInstant *inst_n = temporal_instant_n(temp, n);
            
            char *str = tinstant_out(inst_n, 0);
            
            result.SetValue(i, Value(str));
            
            free(str);
            free(inst_n);
            free(temp);
        }

        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
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
            {LogicalType::VARCHAR, SpanTypes::TSTZSPAN()}, 
            TGeometryTypes::TGEOMETRY(),  
            ExecuteTgeometryFromTstzspan);
        ExtensionUtil::RegisterFunction(instance, tgeometry_from_tstzspan_function);

        // Add the timeSpan function
        auto tgeometry_to_timespan_function = ScalarFunction(
            "timeSpan",
            {TGeometryTypes::TGEOMETRY()},     // takes a tgeompoint
            SpanTypes::TSTZSPAN(),               // returns your SPAN type
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


        auto tgeometryseq_function_2params = ScalarFunction(
            "tgeometrySeq", 
            {TGeometryTypes::TGEOMETRY(), LogicalType::VARCHAR},
            TGeometryTypes::TGEOMETRY(),
            ExecuteTGeometrySeq
        );
        ExtensionUtil::RegisterFunction(instance, tgeometryseq_function_2params);

        auto tgeometryseq_function_1param = ScalarFunction(
            "tgeometrySeq", 
            {TGeometryTypes::TGEOMETRY()},
            TGeometryTypes::TGEOMETRY(),
            ExecuteTGeometrySeq
        );
        ExtensionUtil::RegisterFunction(instance, tgeometryseq_function_1param);

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


        auto setInterp_function = ScalarFunction(
            "setInterp",
            {TGeometryTypes::TGEOMETRY(), LogicalType::VARCHAR},
            TGeometryTypes::TGEOMETRY(),
            ExecuteTGeometrySetInterp
        );
        ExtensionUtil::RegisterFunction(instance, setInterp_function);


        auto merge_function = ScalarFunction(
            "merge",
            {TGeometryTypes::TGEOMETRY(), TGeometryTypes::TGEOMETRY()},
            TGeometryTypes::TGEOMETRY(),
            ExecuteTGeometryMerge
        );
        ExtensionUtil::RegisterFunction(instance, merge_function);

        auto tempSubtype_function = ScalarFunction(
            "tempSubtype",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,
            ExecuteTGeometryTempSubtype
        );
        ExtensionUtil::RegisterFunction(instance, tempSubtype_function);

        auto interp_function = ScalarFunction(
            "interp",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,
            ExecuteTGeometryInterp
        );
        ExtensionUtil::RegisterFunction(instance, interp_function);

        auto memSize_function = ScalarFunction(
            "memSize",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::INTEGER,
            ExecuteTGeometryMemSize
        );
        ExtensionUtil::RegisterFunction(instance, memSize_function);

        auto getValue_function = ScalarFunction(
            "getValue",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,  // Returns geometry as text
            ExecuteTGeometryGetValue
        );
        ExtensionUtil::RegisterFunction(instance, getValue_function);

        auto valueN_function = ScalarFunction(
            "valueN",
            {TGeometryTypes::TGEOMETRY(), LogicalType::INTEGER},
            LogicalType::VARCHAR,  // Returns geometry as text
            ExecuteTGeometryValueN
        );
        ExtensionUtil::RegisterFunction(instance, valueN_function);

        auto endValue_function = ScalarFunction(
            "endValue",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::VARCHAR,  // Returns geometry as text
            ExecuteTGeometryEndValue
        );
        ExtensionUtil::RegisterFunction(instance, endValue_function);

        auto lowerInc_function = ScalarFunction(
            "lowerInc",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::BOOLEAN,
            ExecuteTGeometryLowerInc
        );
        ExtensionUtil::RegisterFunction(instance, lowerInc_function);

        auto upperInc_function = ScalarFunction(
            "upperInc",
            {TGeometryTypes::TGEOMETRY()},
            LogicalType::BOOLEAN,
            ExecuteTGeometryUpperInc
        );
        ExtensionUtil::RegisterFunction(instance, upperInc_function);

        auto startInstant_function = ScalarFunction(
            "startInstant",
            {TGeometryTypes::TGEOMETRY()},
            TGeometryTypes::TGEOMETRY(),  // Returns tgeometry
            ExecuteTGeometryStartInstant
        );
        ExtensionUtil::RegisterFunction(instance, startInstant_function);

        auto instantN_function = ScalarFunction(
            "instantN",
            {TGeometryTypes::TGEOMETRY(), LogicalType::INTEGER},
            TGeometryTypes::TGEOMETRY(),  // Returns tgeometry
            ExecuteTGeometryInstantN
        );
        ExtensionUtil::RegisterFunction(instance, instantN_function);

    }

    void TGeometryTypes::RegisterTypes(DatabaseInstance &instance) {
        ExtensionUtil::RegisterType(instance, "TGEOMETRY", TGeometryTypes::TGEOMETRY());
    }

}