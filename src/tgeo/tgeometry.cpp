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
/*
 * Constructors
*/

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
            
            size_t data_size = temporal_mem_size(tinst);
            
            uint8_t *data_buffer = (uint8_t*)malloc(data_size);
            if (!data_buffer) {
                free(tinst);  
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY data");
            }
            
            memcpy(data_buffer, tinst, data_size);
            
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

inline void ExecuteTgeometryFromTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];
    auto &span_vec = args.data[1];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        input_geom_vec, span_vec, result, count,
        [&](string_t input_geom_str, string_t span_str)-> string_t{
            std::string geom_value = input_geom_str.GetString();

            GSERIALIZED *gs = geom_in(geom_value.c_str(), -1);
            
            if(gs == NULL){
                throw InvalidInputException("Invalid geometry format: "+ geom_value);
            }
            
            const uint8_t *span = reinterpret_cast<const uint8_t*> (span_str.GetData());
            size_t span_size = span_str.GetSize();

            if (span_size < sizeof(void*)){
                throw InvalidInputException("Invalid Span data: insufficient size");
            }
            uint8_t *span_copy = (uint8*) malloc(span_size);
            memcpy(span_copy,span,span_size);
            const Span *span_cmp = reinterpret_cast<const Span*>(span_copy);

            TSequence *seq = tsequence_from_base_tstzspan(Datum(gs), T_TGEOMETRY, span_cmp, STEP);

            if (seq == NULL) {
                free(gs);
                free(span_copy);
                throw InvalidInputException("Failed to create TSequence");
            }

            size_t seq_size = temporal_mem_size((Temporal*)seq);

            uint8_t *seq_buffer = (uint8_t *)malloc(seq_size);
            if (!seq_buffer) {
                free(seq);
                free(gs);
                free(span_copy);
                free((void*)span);
                throw InvalidInputException("Failed to allocate memory for sequence data");
            }

            memcpy(seq_buffer, seq, seq_size);

            string_t seq_string_t((char*) seq_buffer, seq_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, seq_string_t);

            free(seq_buffer);
            free(seq);
            free(span_copy);
            free(gs);

            return stored_data;

        });

    if (count == 1){
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

/*
 * Conversions
*/

inline void ExecuteTGeometryToTimeSpan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> string_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_geom_str.GetData());
            size_t data_size = input_geom_str.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }

            Span *timespan = temporal_to_tstzspan(temp);
            
            if (!timespan) {
                free(data_copy);
                throw InvalidInputException("Failed to extract timespan from TGEOMETRY");
            }
            
            size_t span_size = sizeof(Span);
            
            uint8_t *span_buffer = (uint8_t*)malloc(span_size);
            if (!span_buffer) {
                free(timespan);
                free(data_copy);
                throw InvalidInputException("Failed to allocate memory for timespan data");
            }
            
            memcpy(span_buffer, timespan, span_size);
            
            string_t span_string_t(reinterpret_cast<const char*>(span_buffer), span_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, span_string_t);
            
            free(span_buffer);
            free(timespan);
            free(data_copy);
            
            return stored_data;
        }
    );

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

/*
 * Transformations
*/

inline void ExecuteTGeometryToTInstant(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> string_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_geom_str.GetData());
            size_t data_size = input_geom_str.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }

            TInstant *inst = temporal_to_tinstant(temp);
            if (!inst) {
                free(data_copy);
                throw InvalidInputException("Failed to convert TGEOMETRY to TInstant");
            }
            
            size_t inst_size = temporal_mem_size((Temporal*)inst);
            
            uint8_t *inst_buffer = (uint8_t*)malloc(inst_size);
            if (!inst_buffer) {
                free(inst);
                free(data_copy);
                throw InvalidInputException("Failed to allocate memory for TInstant data");
            }
            
            memcpy(inst_buffer, inst, inst_size);
            
            string_t inst_string_t(reinterpret_cast<const char*>(inst_buffer), inst_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, inst_string_t);
            
            free(inst_buffer);
            free(inst);
            free(data_copy);
            
            return stored_data;
        }
    );

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}


inline void ExecuteTGeometrySetInterp(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tgeom_vec = args.data[0];
    auto &interp_vec = args.data[1];

    tgeom_vec.Flatten(count);
    interp_vec.Flatten(count);

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        tgeom_vec, interp_vec, result, count,
        [&](string_t tgeom_str_t, string_t interp_str_t) -> string_t {
          
            const uint8_t *data = reinterpret_cast<const uint8_t*>(tgeom_str_t.GetData());
            size_t data_size = tgeom_str_t.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }

            // Get interpolation string
            std::string interp_str = interp_str_t.GetString();
            interpType new_interp = interptype_from_string(interp_str.c_str());
            
            Temporal *result_temp = temporal_set_interp(temp, new_interp);
            if (!result_temp) {
                free(data_copy);
                throw InvalidInputException("Failed to set interpolation");
            }
            
            // Serialize result back to binary
            size_t result_size = temporal_mem_size(result_temp);
            uint8_t *result_buffer = (uint8_t*)malloc(result_size);
            if (!result_buffer) {
                free(result_temp);
                free(data_copy);
                throw InvalidInputException("Failed to allocate memory for result");
            }
            
            memcpy(result_buffer, result_temp, result_size);
            string_t result_string_t(reinterpret_cast<const char*>(result_buffer), result_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, result_string_t);
            
            free(result_buffer);
            free(result_temp);
            free(data_copy);
            
            return stored_data;
        });

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

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        tgeom1_vec, tgeom2_vec, result, count,
        [&](string_t tgeom1_str_t, string_t tgeom2_str_t) -> string_t {
            // Deserialize first TGEOMETRY
            const uint8_t *data1 = reinterpret_cast<const uint8_t*>(tgeom1_str_t.GetData());
            size_t data1_size = tgeom1_str_t.GetSize();

            if (data1_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data1_copy = (uint8_t*)malloc(data1_size);
            if (!data1_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data1_copy, data1, data1_size);

            Temporal *temp1 = reinterpret_cast<Temporal*>(data1_copy);
            if (!temp1) {
                free(data1_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }

            // Deserialize second TGEOMETRY
            const uint8_t *data2 = reinterpret_cast<const uint8_t*>(tgeom2_str_t.GetData());
            size_t data2_size = tgeom2_str_t.GetSize();

            if (data2_size < sizeof(void*)) {
                free(data1_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data2_copy = (uint8_t*)malloc(data2_size);
            if (!data2_copy) {
                free(data1_copy);
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data2_copy, data2, data2_size);

            Temporal *temp2 = reinterpret_cast<Temporal*>(data2_copy);
            if (!temp2) {
                free(data1_copy);
                free(data2_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }
            
            Temporal *result_temp = temporal_merge(temp1, temp2);
            if (!result_temp) {
                free(data1_copy);
                free(data2_copy);
                throw InvalidInputException("Failed to merge temporal geometries");
            }
            
            // Serialize result back to binary
            size_t result_size = temporal_mem_size(result_temp);
            uint8_t *result_buffer = (uint8_t*)malloc(result_size);
            if (!result_buffer) {
                free(result_temp);
                free(data1_copy);
                free(data2_copy);
                throw InvalidInputException("Failed to allocate memory for result");
            }
            
            memcpy(result_buffer, result_temp, result_size);
            string_t result_string_t(reinterpret_cast<const char*>(result_buffer), result_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, result_string_t);
            
            free(result_buffer);
            free(result_temp);
            free(data1_copy);
            free(data2_copy);
            
            return stored_data;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}


/*
 * Accessor Functions
*/

inline void ExecuteTGeometryTempSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tgeom_vec = args.data[0];

    tgeom_vec.Flatten(count);

    UnaryExecutor::Execute<string_t, string_t>(
        tgeom_vec, result, count,
        [&](string_t tgeom_str_t) -> string_t {
            // Deserialize TGEOMETRY from binary data
            const uint8_t *data = reinterpret_cast<const uint8_t*>(tgeom_str_t.GetData());
            size_t data_size = tgeom_str_t.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }
            
            const char *subtype_str = temporal_subtype(temp);
            if (!subtype_str) {
                free(data_copy);
                throw InvalidInputException("Failed to get temporal subtype");
            }

            std::string result_str(subtype_str);
            string_t stored_result = StringVector::AddString(result, result_str);
            
            free(data_copy);
            
            return stored_result;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}




inline void ExecuteTGeometryInterp(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tgeom_vec = args.data[0];

    tgeom_vec.Flatten(count);

    UnaryExecutor::Execute<string_t, string_t>(
        tgeom_vec, result, count,
        [&](string_t tgeom_str_t) -> string_t {
            // Deserialize TGEOMETRY from binary data
            const uint8_t *data = reinterpret_cast<const uint8_t*>(tgeom_str_t.GetData());
            size_t data_size = tgeom_str_t.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }
            
            const char *interp_str = temporal_interp(temp);
            if (!interp_str) {
                free(data_copy);
                throw InvalidInputException("Failed to get temporal interpolation");
            }

            std::string result_str(interp_str);
            string_t stored_result = StringVector::AddString(result, result_str);
            
            free(data_copy);
            
            return stored_result;
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeometryMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tgeom_vec = args.data[0];

    tgeom_vec.Flatten(count);

    UnaryExecutor::Execute<string_t, int32_t>(
        tgeom_vec, result, count,
        [&](string_t tgeom_str_t) -> int32_t {
            // Deserialize TGEOMETRY from binary data
            const uint8_t *data = reinterpret_cast<const uint8_t*>(tgeom_str_t.GetData());
            size_t data_size = tgeom_str_t.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }
            
            size_t mem_size = temporal_mem_size(temp);
            
            free(data_copy);
            
            return static_cast<int32_t>(mem_size);
        });

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}




inline void ExecuteTGeometryToTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, timestamp_tz_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> timestamp_tz_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_geom_str.GetData());
            size_t data_size = input_geom_str.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMETRY data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMETRY deserialization");
            }
            memcpy(data_copy, data, data_size);

            TInstant *temp = reinterpret_cast<TInstant*>(data_copy);
            
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMETRY data: null pointer");
            }

            TimestampTz meos_t = temp->t;
            
            timestamp_tz_t duckdb_t = static_cast<timestamp_tz_t>(meos_t + 946684800000000LL);
            
            free(data_copy);
            
            return duckdb_t;
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

     auto tgeometry_from_tstzspan_function = ScalarFunction(
        "TGEOMETRY", 
        {LogicalType::VARCHAR, SpanTypes::TSTZSPAN()}, 
        TGeometryTypes::TGEOMETRY(),  
        ExecuteTgeometryFromTstzspan);
    ExtensionUtil::RegisterFunction(instance, tgeometry_from_tstzspan_function);

    auto tgeometry_to_timespan_function = ScalarFunction(
        "timeSpan",
        {TGeometryTypes::TGEOMETRY()},     
        SpanTypes::TSTZSPAN(),               
        ExecuteTGeometryToTimeSpan);
    ExtensionUtil::RegisterFunction(instance, tgeometry_to_timespan_function);

    auto tgeometry_to_tinstant_function = ScalarFunction(
        "tgeometryInst",
        {TGeometryTypes::TGEOMETRY()},
        TGeometryTypes::TGEOMETRY(),  
        ExecuteTGeometryToTInstant);
    ExtensionUtil::RegisterFunction(instance, tgeometry_to_tinstant_function);


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


    auto tgeometry_gettimestamptz_function = ScalarFunction(
        "getTimestamp",
        {TGeometryTypes::TGEOMETRY()},
        LogicalType::TIMESTAMP_TZ,  
        ExecuteTGeometryToTimestamp);
    ExtensionUtil::RegisterFunction(instance, tgeometry_gettimestamptz_function);

}

void TGeometryTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TGEOMETRY", TGeometryTypes::TGEOMETRY());
}


}