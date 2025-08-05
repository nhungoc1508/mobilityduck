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



TInstant **temparr_extract(Vector &tgeometry_arr_vec, list_entry_t list_entry, int *count) {
    auto &child_vector = ListVector::GetEntry(tgeometry_arr_vec);
    auto list_size = list_entry.length;
    auto list_offset = list_entry.offset;
    
    if (list_size == 0) {
        *count = 0;
        return nullptr;
    }
    
    *count = list_size;
    
    // Allocate array for TInstant pointers
    TInstant **instants = (TInstant**)malloc(sizeof(TInstant*) * list_size);
    
    // Extract each TGEOMETRY element and convert to TInstant
    for (idx_t i = 0; i < list_size; i++) {
        auto element_idx = list_offset + i;
        string_t tgeom_blob = FlatVector::GetData<string_t>(child_vector)[element_idx];
        
        // Convert WKB blob back to Temporal/TInstant
        const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
        size_t wkb_size = tgeom_blob.GetSize();
        
        Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
        if (!temp) {
            // Clean up previously allocated instants
            for (idx_t j = 0; j < i; j++) {
                if (instants[j]) free(instants[j]);
            }
            free(instants);
            *count = 0;
            return nullptr;
        }
        
        instants[i] = (TInstant*)temp;
    }
    
    return instants;
}


inline void ExecuteTGeometrySeq(DataChunk &args, ExpressionState &state, Vector &result) {
    const char* default_interp = "step";
    auto count = args.size();
    auto &tgeometry_vec = args.data[0];
    
    // Create a constant vector with default interpolation if no second argument
    Vector interp_vec(LogicalType::VARCHAR, count);
    if (args.data.size() > 1) {
        interp_vec.Reference(args.data[1]);
    } else {
        // Fill with default interpolation
        for (idx_t i = 0; i < count; i++) {
            FlatVector::GetData<string_t>(interp_vec)[i] = StringVector::AddString(interp_vec, default_interp);
        }
    }
    
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        tgeometry_vec, interp_vec, result, count,
        [&](string_t tgeom_blob, string_t interp_str) -> string_t {
            // Convert WKB blob back to Temporal
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
            size_t wkb_size = tgeom_blob.GetSize();
            
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (!temp) {
                throw InvalidInputException("Failed to convert TGEOMETRY to temporal object");
            }
            
            std::string interp_string = interp_str.GetString();
            if (interp_string.empty()) {
                interp_string = default_interp;
            }
            
            interpType interp = interptype_from_string(interp_string.c_str());
            TSequence *seq = temporal_to_tsequence(temp, interp);
            
            if (!seq) {
                free(temp);
                throw InvalidInputException("Failed to create TSequence");
            }
            
            // Convert TSequence to WKB for consistent storage
            size_t seq_wkb_size;
            uint8_t *seq_wkb_data = temporal_as_wkb(reinterpret_cast<Temporal*>(seq), 0, &seq_wkb_size);
            
            if (!seq_wkb_data) {
                free(temp);
                free(seq);
                throw InvalidInputException("Failed to convert TSequence to WKB format");
            }
            
            // Create string_t from binary data and store as BLOB
            string_t wkb_string_t(reinterpret_cast<const char*>(seq_wkb_data), seq_wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            // Clean up
            free(seq_wkb_data);
            free(temp);
            free(seq);
            
            return stored_data;
        });
    
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeometrySeqArr(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tgeometry_arr_vec = args.data[0];    
    auto &interp_vec = args.data[1];
    
    BinaryExecutor::Execute<list_entry_t, string_t, string_t>(
        tgeometry_arr_vec, interp_vec, result, count,
        [&](list_entry_t list_entry, string_t interp_str) -> string_t {
            std::string interp_string = interp_str.GetString();
            interpType interp = interptype_from_string(interp_string.c_str());
            
            // Use default values of true for lower and upper bounds
            bool lower_inc = true;
            bool upper_inc = true;
            
            // Extract array elements using the new function
            int element_count;
            TInstant **instants = temparr_extract(tgeometry_arr_vec, list_entry, &element_count);
            
            if (!instants || element_count == 0) {
                throw InvalidInputException("Failed to extract TInstant array from input");
            }
            
            TSequence *sequence_result = tsequence_make((const TInstant **) instants, element_count, 
                                                    lower_inc, upper_inc, interp, true);
            
            if (!sequence_result) {
                // Clean up before throwing
                for (int j = 0; j < element_count; j++) {
                    if (instants[j]) {
                        free(instants[j]);
                    }
                }
                free(instants);
                throw InvalidInputException("Failed to create TSequence");
            }
            
            // Convert to WKB binary format for consistent storage
            size_t wkb_size;
            uint8_t *wkb_data = temporal_as_wkb(reinterpret_cast<Temporal*>(sequence_result), 0, &wkb_size);
            
            if (!wkb_data) {
                free(sequence_result);
                for (int j = 0; j < element_count; j++) {
                    if (instants[j]) {
                        free(instants[j]);
                    }
                }
                free(instants);
                throw InvalidInputException("Failed to convert TSequence to WKB format");
            }
            
            // Create string_t from binary data and store as BLOB
            string_t wkb_string_t(reinterpret_cast<const char*>(wkb_data), wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string_t);
            
            // Clean up memory
            free(wkb_data);
            free(sequence_result);
            for (int j = 0; j < element_count; j++) {
                if (instants[j]) {
                    free(instants[j]);
                }
            }
            free(instants);
            
            return stored_data;
        });
    
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}


inline void ExecuteTGeometryAsEWKT(DataChunk &args, ExpressionState &state, Vector &result){
    auto count = args.size();
    auto &input_geom_vec = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_geom_vec, result, count,
        [&](string_t input_geom_str) -> string_t {

            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*> (input_geom_str.GetData());
            size_t wkb_size = input_geom_str.GetSize();

            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);

            if(temp == NULL){
                throw InvalidInputException("Invalid WKB data for TGEOMETRY");
            }

            char *ewkt = tspatial_as_ewkt(temp, 0);
            
            if (ewkt == NULL) {
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMETRY to text");
            }
            
            std::string result_str(ewkt);
            free(ewkt);
            free(temp);
            
            return StringVector::AddString(result, result_str);

        }
    );

    if (count == 1){
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }

}

 


bool TgeometryFunctions::StringToTgeometry(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_geom_str) -> string_t {
            std::string input_str = input_geom_str.GetString();

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

    auto tgeometryseqarr_function = ScalarFunction(
            "tgeometrySeq", 
            {LogicalType::LIST(TGeometryTypes::TGEOMETRY()), LogicalType::VARCHAR},
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

void TGeometryTypes::RegisterCastFunctions(DatabaseInstance &db) {
    ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, TGeometryTypes::TGEOMETRY(), TgeometryFunctions::StringToTgeometry);
    ExtensionUtil::RegisterCastFunction(db, TGeometryTypes::TGEOMETRY(), LogicalType::VARCHAR, TgeometryFunctions::TgeometryToString);
}
}
