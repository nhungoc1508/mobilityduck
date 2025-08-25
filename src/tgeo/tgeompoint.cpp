#include "tgeompoint.hpp"
#include "time_util.hpp"
#include "temporal/temporal_functions.hpp"
#include "temporal/temporal.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include <regex>
#include <string>
#include <span.hpp>
#include <spanset.hpp>
#include <geo/stbox.hpp>

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

inline void ExecuteTGeomPointFromGeom(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, timestamp_tz_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t geom_str, timestamp_tz_t ts_duckdb) {
            std::string geom_val = geom_str.GetString();
            if (geom_val.empty()) {
                throw InvalidInputException("Empty geometry string");
            }

            GSERIALIZED *gs = geom_in(geom_val.c_str(), -1);
            if (gs == NULL) {
                throw InvalidInputException("Invalid geometry format: " + geom_val);
            }

            timestamp_tz_t ts_meos = DuckDBToMeosTimestamp(ts_duckdb);
            Temporal *ret = (Temporal *) tpointinst_make(gs, static_cast<TimestampTz>(ts_meos.value));
            if (ret == NULL) {
                free(gs);
                throw InvalidInputException("Failed to create TGEOMPOINT from geometry and timestamp");
            }
            size_t ret_size = temporal_mem_size(ret);
            char *ret_data = (char *)malloc(ret_size);
            if (!ret_data) {
                free(ret);
                free(gs);
                throw InvalidInputException("Failed to allocate memory for TGEOMPOINT");
            }
            memcpy(ret_data, ret, ret_size);
            
            free(ret);
            free(gs);

            string_t temp_str(ret_data, ret_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, temp_str);
            free(ret_data);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointFromWKB(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, timestamp_tz_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t wkb_blob, timestamp_tz_t ts_duckdb) {
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(wkb_blob.GetData());
            size_t wkb_size = wkb_blob.GetSize();
            if (!wkb_data || wkb_size == 0) {
                throw InvalidInputException("Empty WKB_BLOB input");
            }

            int32 srid = 0;
            GSERIALIZED *gs = geo_from_ewkb(wkb_data, wkb_size, srid);
            if (!gs) {
                throw InvalidInputException("Failed to parse WKB_BLOB into a geometry");
            }

            timestamp_tz_t ts_meos = DuckDBToMeosTimestamp(ts_duckdb);
            Temporal *ret = (Temporal *) tpointinst_make(gs, static_cast<TimestampTz>(ts_meos.value));
            if (ret == NULL) {
                free(gs);
                throw InvalidInputException("Failed to create TGEOMPOINT from geometry and timestamp");
            }

            /* Output MEOS binary */
            size_t ret_size = temporal_mem_size(ret);
            char *ret_data = (char *)malloc(ret_size);
            if (!ret_data) {
                free(ret);
                free(gs);
                throw InvalidInputException("Failed to allocate memory for TGEOMPOINT");
            }
            memcpy(ret_data, ret, ret_size);
            
            free(ret);
            free(gs);

            string_t temp_str(ret_data, ret_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, temp_str);
            free(ret_data);
            return stored_data;
            

            /* Output WKB 
            size_t out_wkb_size;
            uint8_t *out_wkb_data = temporal_as_wkb(ret, WKB_EXTENDED, &out_wkb_size);
            string_t wkb_string(reinterpret_cast<const char *>(out_wkb_data), out_wkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, wkb_string);
            free(out_wkb_data);
            free(ret);
            free(gs);
            return stored_data;
            */
        }
    );
    if (args.size() == 1) {
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

bool TgeomPointFunctions::StringToTgeomPointNew(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_geom_str) -> string_t {
            std::string input_str = input_geom_str.GetString();

            Temporal *temp = tgeompoint_in(input_str.c_str());
            if (!temp) {
                throw InvalidInputException("Invalid TGEOMPOINT input: " + input_str);
            }

            size_t data_size = temporal_mem_size(temp);
            uint8_t *data_buffer = (uint8_t *)malloc(temporal_mem_size(temp));
            if (!data_buffer) {
                free(temp);
                throw InvalidInputException("Failed to allocate memory for TGEOMPOINT");
            }

            memcpy(data_buffer, temp, data_size);

            string_t output(reinterpret_cast<const char *>(data_buffer), data_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, output);

            free(data_buffer);
            free(temp);
            return stored_data;
        }
    );
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

bool TgeomPointFunctions::TgeomPointToStringNew(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_blob) {
            /* Input MEOS binary */
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();

            if (data_size < sizeof(void*)) {
                throw InvalidInputException("Invalid TGEOMPOINT data: insufficient size");
            }

            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            if (!data_copy) {
                throw InvalidInputException("Failed to allocate memory for TGEOMPOINT deserialization");
            }
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            char *str = temporal_out(temp, 15);
            if (!str) {
                free(data_copy);
                throw InvalidInputException("Failed to convert TGEOMPOINT to string");
            }

            std::string output(str);
            string_t stored_result = StringVector::AddString(result, output);

            free(str);
            free(data_copy);
            return stored_result;
            

            /* Input WKB 
            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t wkb_size = input_blob.GetSize();
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
            if (!temp) {
                throw InvalidInputException("Invalid WKB data for TGEOMPOINT");
            }

            char *str = temporal_out(temp, 15);
            if (!str) {
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMPOINT to string");
            }

            std::string output(str);
            string_t stored_result = StringVector::AddString(result, output);

            free(str);
            free(temp);
            return stored_result;
            */
        }
    );
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

inline void ExecuteTGeomPointTrajectory(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input_blob) -> string_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            GSERIALIZED *gs = tpoint_trajectory(temp);
            if (!gs) {
                free(temp);
                throw InvalidInputException("Failed to get trajectory from TGEOMPOINT");
            }

            size_t ewkb_size;
            uint8_t *ewkb_data = geo_as_ewkb(gs, NULL, &ewkb_size);
            if (!ewkb_data) {
                free(temp);
                throw InvalidInputException("Failed to convert trajectory to EWKB");
            }

            string_t ewkb_string(reinterpret_cast<const char*>(ewkb_data), ewkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, ewkb_string);

            free(ewkb_data);
            free(gs);
            free(temp);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointAtTime(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input_blob, string_t input_span, ValidityMask &mask, idx_t idx) -> string_t {
            const uint8_t *tgeom_data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t tgeom_data_size = input_blob.GetSize();
            uint8_t *tgeom_data_copy = (uint8_t*)malloc(tgeom_data_size);
            memcpy(tgeom_data_copy, tgeom_data, tgeom_data_size);
            Temporal *tgeom = reinterpret_cast<Temporal*>(tgeom_data_copy);
            if (!tgeom) {
                free(tgeom_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *span_data = reinterpret_cast<const uint8_t*>(input_span.GetData());
            size_t span_data_size = input_span.GetSize();
            uint8_t *span_data_copy = (uint8_t*)malloc(span_data_size);
            memcpy(span_data_copy, span_data, span_data_size);
            Span *span = reinterpret_cast<Span*>(span_data_copy);
            if (!span) {
                free(span_data_copy);
                throw InvalidInputException("Invalid TSTZSPAN data: null pointer");
            }

            Temporal *ret = temporal_restrict_tstzspan(tgeom, span, true);
            if (!ret) {
                free(tgeom);
                free(span);
                mask.SetInvalid(idx);
                return string_t();
            }
            
            size_t ret_size = temporal_mem_size(ret);
            uint8_t *ret_data = (uint8_t*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);

            string_t ret_string(reinterpret_cast<const char*>(ret_data), ret_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, ret_string);

            free(ret_data);
            free(ret);
            free(tgeom);
            free(span);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointAtTstzspanset(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t tgeom_blob, string_t spanset_blob) -> string_t {
            const uint8_t *tgeom_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
            size_t tgeom_data_size = tgeom_blob.GetSize();
            uint8_t *tgeom_data_copy = (uint8_t*)malloc(tgeom_data_size);
            memcpy(tgeom_data_copy, tgeom_data, tgeom_data_size);
            Temporal *tgeom = reinterpret_cast<Temporal*>(tgeom_data_copy);
            if (!tgeom) {
                free(tgeom_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *spanset_data = reinterpret_cast<const uint8_t*>(spanset_blob.GetData());
            size_t spanset_data_size = spanset_blob.GetSize();
            uint8_t *spanset_data_copy = (uint8_t*)malloc(spanset_data_size);
            memcpy(spanset_data_copy, spanset_data, spanset_data_size);
            SpanSet *spanset = reinterpret_cast<SpanSet*>(spanset_data_copy);
            if (!spanset) {
                free(spanset_data_copy);
                throw InvalidInputException("Invalid TSTZSPANSET data: null pointer");
            }

            Temporal *ret = temporal_at_tstzspanset(tgeom, spanset);
            if (!ret) {
                free(tgeom);
                free(spanset);
                throw InvalidInputException("Failed to restrict TGEOMPOINT to TSTZSPANSET");
            }
            
            size_t ret_size = temporal_mem_size(ret);
            uint8_t *ret_data = (uint8_t*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);

            string_t ret_string(reinterpret_cast<const char*>(ret_data), ret_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, ret_string);

            free(ret_data);
            free(ret);
            free(tgeom);
            free(spanset);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointAtValues(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t tgeom_blob, string_t wkb_blob, ValidityMask &mask, idx_t idx) -> string_t {
            const uint8_t *tgeom_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
            size_t tgeom_data_size = tgeom_blob.GetSize();
            uint8_t *tgeom_data_copy = (uint8_t*)malloc(tgeom_data_size);
            memcpy(tgeom_data_copy, tgeom_data, tgeom_data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(tgeom_data_copy);
            if (!temp) {
                free(tgeom_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *wkb_data = reinterpret_cast<const uint8_t*>(wkb_blob.GetData());
            size_t wkb_size = wkb_blob.GetSize();
            int32 srid = 0;
            GSERIALIZED *gs = geo_from_ewkb(wkb_data, wkb_size, srid);
            if (!gs) {
                free(temp);
                throw InvalidInputException("Invalid geometry format: " + wkb_blob.GetString());
            }

            Temporal *ret = temporal_restrict_value(temp, (Datum)gs, true);
            if (!ret) {
                free(temp);
                free(gs);
                mask.SetInvalid(idx);
                return string_t();
            }

            size_t ret_size = temporal_mem_size(ret);
            uint8_t *ret_data = (uint8_t*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);

            string_t ret_string(reinterpret_cast<const char*>(ret_data), ret_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, ret_string);
            
            free(ret_data);
            free(ret);
            free(temp);
            free(gs);
            return stored_data;
        }
    );
}

inline void ExecuteTGeomPointStartTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, timestamp_tz_t>(
        args.data[0], result, args.size(),
        [&](string_t input_blob) -> timestamp_tz_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            TimestampTz start_ts_meos = temporal_start_timestamptz(temp);
            timestamp_tz_t start_ts_duckdb = MeosToDuckDBTimestamp((timestamp_tz_t)start_ts_meos);

            free(temp);
            return start_ts_duckdb;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointValueAtTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, timestamp_tz_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input_blob, timestamp_tz_t ts_duckdb, ValidityMask &mask, idx_t idx) -> string_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            timestamp_tz_t ts_meos = DuckDBToMeosTimestamp(ts_duckdb);
            Datum ret;
            bool found = temporal_value_at_timestamptz(temp, (TimestampTz)ts_meos.value, true, &ret);
            free(temp);
            if (!found) {
                mask.SetInvalid(idx);
                return string_t();
            }

            GSERIALIZED *gs = DatumGetGserializedP(ret);
            if (!gs) {
                throw InvalidInputException("Failed to get geometry from datum");
            }

            size_t ewkb_size;
            uint8_t *ewkb_data = geo_as_ewkb(gs, NULL, &ewkb_size);
            if (!ewkb_data) {
                free(gs);
                throw InvalidInputException("Failed to convert geometry to EWKB");
            }

            string_t ewkb_string(reinterpret_cast<const char*>(ewkb_data), ewkb_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, ewkb_string);

            free(ewkb_data);
            free(gs);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointEdwithin(DataChunk &args, ExpressionState &state, Vector &result) {
    TernaryExecutor::ExecuteWithNulls<string_t, string_t, double, bool>(
        args.data[0], args.data[1], args.data[2], result, args.size(),
        [&](string_t tgeom1_blob, string_t tgeom2_blob, double dist, ValidityMask &mask, idx_t idx) -> bool {
            const uint8_t *tgeom1_data = reinterpret_cast<const uint8_t*>(tgeom1_blob.GetData());
            size_t tgeom1_data_size = tgeom1_blob.GetSize();
            uint8_t *tgeom1_data_copy = (uint8_t*)malloc(tgeom1_data_size);
            memcpy(tgeom1_data_copy, tgeom1_data, tgeom1_data_size);
            Temporal *tgeom1 = reinterpret_cast<Temporal*>(tgeom1_data_copy);
            if (!tgeom1) {
                free(tgeom1_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *tgeom2_data = reinterpret_cast<const uint8_t*>(tgeom2_blob.GetData());
            size_t tgeom2_data_size = tgeom2_blob.GetSize();
            uint8_t *tgeom2_data_copy = (uint8_t*)malloc(tgeom2_data_size);
            memcpy(tgeom2_data_copy, tgeom2_data, tgeom2_data_size);
            Temporal *tgeom2 = reinterpret_cast<Temporal*>(tgeom2_data_copy);
            if (!tgeom2) {
                free(tgeom2_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            int ret = edwithin_tgeo_tgeo(tgeom1, tgeom2, dist);
            free(tgeom1);
            free(tgeom2);
            if (ret < 0) {
                mask.SetInvalid(idx);
                return false;
            }
            return ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointTdwithin(DataChunk &args, ExpressionState &state, Vector &result) {
    TernaryExecutor::ExecuteWithNulls<string_t, string_t, double, string_t>(
        args.data[0], args.data[1], args.data[2], result, args.size(),
        [&](string_t tgeom1_blob, string_t tgeom2_blob, double dist, ValidityMask &mask, idx_t idx) -> string_t {
            const uint8_t *tgeom1_data = reinterpret_cast<const uint8_t*>(tgeom1_blob.GetData());
            size_t tgeom1_data_size = tgeom1_blob.GetSize();
            uint8_t *tgeom1_data_copy = (uint8_t*)malloc(tgeom1_data_size);
            memcpy(tgeom1_data_copy, tgeom1_data, tgeom1_data_size);
            Temporal *tgeom1 = reinterpret_cast<Temporal*>(tgeom1_data_copy);
            if (!tgeom1) {
                free(tgeom1_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *tgeom2_data = reinterpret_cast<const uint8_t*>(tgeom2_blob.GetData());
            size_t tgeom2_data_size = tgeom2_blob.GetSize();
            uint8_t *tgeom2_data_copy = (uint8_t*)malloc(tgeom2_data_size);
            memcpy(tgeom2_data_copy, tgeom2_data, tgeom2_data_size);
            Temporal *tgeom2 = reinterpret_cast<Temporal*>(tgeom2_data_copy);
            if (!tgeom2) {
                free(tgeom2_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }
            // extern Temporal *tdwithin_tgeo_tgeo(const Temporal *temp1, const Temporal *temp2, double dist, bool restr, bool atvalue);
            Temporal *ret = tdwithin_tgeo_tgeo(tgeom1, tgeom2, dist, false, false);
            if (!ret) {
                free(tgeom1);
                free(tgeom2);
                mask.SetInvalid(idx);
                return string_t();
            }
            size_t ret_size = temporal_mem_size(ret);
            uint8_t *ret_data = (uint8_t*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t ret_string(reinterpret_cast<const char*>(ret_data), ret_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, ret_string);
            free(ret_data);
            free(ret);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTGeomPointAdisjoint(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, string_t, bool>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t tgeom1_blob, string_t tgeom2_blob, ValidityMask &mask, idx_t idx) -> bool {
            const uint8_t *tgeom1_data = reinterpret_cast<const uint8_t*>(tgeom1_blob.GetData());
            size_t tgeom1_data_size = tgeom1_blob.GetSize();
            uint8_t *tgeom1_data_copy = (uint8_t*)malloc(tgeom1_data_size);
            memcpy(tgeom1_data_copy, tgeom1_data, tgeom1_data_size);
            Temporal *tgeom1 = reinterpret_cast<Temporal*>(tgeom1_data_copy);
            if (!tgeom1) {
                free(tgeom1_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }
            
            const uint8_t *tgeom2_data = reinterpret_cast<const uint8_t*>(tgeom2_blob.GetData());
            size_t tgeom2_data_size = tgeom2_blob.GetSize();
            uint8_t *tgeom2_data_copy = (uint8_t*)malloc(tgeom2_data_size);
            memcpy(tgeom2_data_copy, tgeom2_data, tgeom2_data_size);
            Temporal *tgeom2 = reinterpret_cast<Temporal*>(tgeom2_data_copy);
            if (!tgeom2) {
                free(tgeom2_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            // extern int adisjoint_tgeo_tgeo(const Temporal *temp1, const Temporal *temp2);
            int ret = adisjoint_tgeo_tgeo(tgeom1, tgeom2);
            free(tgeom1);
            free(tgeom2);
            if (ret < 0) {
                mask.SetInvalid(idx);
                return false;
            }
            return ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void Tgeompoint_to_stbox_common(Vector &source, Vector &result, idx_t count) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_blob) -> string_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
                return string_t();
            }
            STBox *stbox = tspatial_to_stbox(temp);
            if (!stbox) {
                free(temp);
                throw InvalidInputException("Failed to convert TGEOMPOINT to STBOX");
                return string_t();
            }

            size_t stbox_size = sizeof(STBox);
            uint8_t *stbox_data = (uint8_t*)malloc(stbox_size);
            if (!stbox_data) {
                free(temp);
                throw InvalidInputException("Failed to allocate memory for STBOX");
                return string_t();
            }
            memcpy(stbox_data, stbox, stbox_size);
            string_t stbox_string(reinterpret_cast<const char*>(stbox_data), stbox_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, stbox_string);

            free(stbox_data);
            free(stbox);
            free(temp);
            return stored_data;
        }
    );
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void Tgeompoint_to_stbox(DataChunk &args, ExpressionState &state, Vector &result) {
    Tgeompoint_to_stbox_common(args.data[0], result, args.size());
}

inline bool Tgeompoint_to_stbox_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    Tgeompoint_to_stbox_common(source, result, count);
    return true;
}

inline void Temporal_overlaps_tgeompoint_stbox(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, bool>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t tgeom_blob, string_t stbox_blob) -> bool {
            const uint8_t *tgeom_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
            size_t tgeom_data_size = tgeom_blob.GetSize();
            uint8_t *tgeom_data_copy = (uint8_t*)malloc(tgeom_data_size);
            memcpy(tgeom_data_copy, tgeom_data, tgeom_data_size);
            Temporal *tgeom = reinterpret_cast<Temporal*>(tgeom_data_copy);
            if (!tgeom) {
                free(tgeom_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *stbox_data = reinterpret_cast<const uint8_t*>(stbox_blob.GetData());
            size_t stbox_data_size = stbox_blob.GetSize();
            uint8_t *stbox_data_copy = (uint8_t*)malloc(stbox_data_size);
            memcpy(stbox_data_copy, stbox_data, stbox_data_size);
            STBox *stbox = reinterpret_cast<STBox*>(stbox_data_copy);
            if (!stbox) {
                free(stbox_data_copy);
                throw InvalidInputException("Invalid STBOX data: null pointer");
            }
            bool ret = overlaps_tspatial_stbox(tgeom, stbox);
            free(tgeom);
            free(stbox);
            return ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void Temporal_overlaps_tgeompoint_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, bool>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t tgeom_blob, string_t span_blob) -> bool {
            const uint8_t *tgeom_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
            size_t tgeom_data_size = tgeom_blob.GetSize();
            uint8_t *tgeom_data_copy = (uint8_t*)malloc(tgeom_data_size);
            memcpy(tgeom_data_copy, tgeom_data, tgeom_data_size);
            Temporal *tgeom = reinterpret_cast<Temporal*>(tgeom_data_copy);
            if (!tgeom) {
                free(tgeom_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }
            
            const uint8_t *span_data = reinterpret_cast<const uint8_t*>(span_blob.GetData());
            size_t span_data_size = span_blob.GetSize();
            uint8_t *span_data_copy = (uint8_t*)malloc(span_data_size);
            memcpy(span_data_copy, span_data, span_data_size);
            Span *span = reinterpret_cast<Span*>(span_data_copy);
            if (!span) {
                free(span_data_copy);
                throw InvalidInputException("Invalid TSTZSPAN data: null pointer");
            }
            bool ret = overlaps_tstzspan_temporal(span, tgeom);
            free(tgeom);
            free(span);
            return ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void Temporal_contains_tgeompoint_stbox(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, bool>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t tgeom_blob, string_t stbox_blob) -> bool {
            const uint8_t *tgeom_data = reinterpret_cast<const uint8_t*>(tgeom_blob.GetData());
            size_t tgeom_data_size = tgeom_blob.GetSize();
            uint8_t *tgeom_data_copy = (uint8_t*)malloc(tgeom_data_size);
            memcpy(tgeom_data_copy, tgeom_data, tgeom_data_size);
            Temporal *tgeom = reinterpret_cast<Temporal*>(tgeom_data_copy);
            if (!tgeom) {
                free(tgeom_data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }

            const uint8_t *stbox_data = reinterpret_cast<const uint8_t*>(stbox_blob.GetData());
            size_t stbox_data_size = stbox_blob.GetSize();
            uint8_t *stbox_data_copy = (uint8_t*)malloc(stbox_data_size);
            memcpy(stbox_data_copy, stbox_data, stbox_data_size);
            STBox *stbox = reinterpret_cast<STBox*>(stbox_data_copy);
            if (!stbox) {
                free(stbox_data_copy);
                throw InvalidInputException("Invalid STBOX data: null pointer");
            }
            bool ret = contains_tspatial_stbox(tgeom, stbox);
            free(tgeom);
            free(stbox);
            return ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void Tgeompoint_length(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, double>(
        args.data[0], result, args.size(),
        [&](string_t input_blob) -> double {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }
            double ret = tpoint_length(temp);
            free(temp);
            return ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void Tgeompoint_get_time(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input_blob) -> string_t {
            const uint8_t *data = reinterpret_cast<const uint8_t*>(input_blob.GetData());
            size_t data_size = input_blob.GetSize();
            uint8_t *data_copy = (uint8_t*)malloc(data_size);
            memcpy(data_copy, data, data_size);
            Temporal *temp = reinterpret_cast<Temporal*>(data_copy);
            if (!temp) {
                free(data_copy);
                throw InvalidInputException("Invalid TGEOMPOINT data: null pointer");
            }
            SpanSet *ret = temporal_time(temp);
            if (!ret) {
                free(temp);
                throw InvalidInputException("Failed to get time from TGEOMPOINT");
            }
            size_t span_set_size = VARSIZE(ret);
            uint8_t *span_set_data = (uint8_t*)malloc(span_set_size);
            if (!span_set_data) {
                free(temp);
                throw InvalidInputException("Failed to allocate memory for SpanSet");
            }
            memcpy(span_set_data, ret, span_set_size);
            string_t span_set_string(reinterpret_cast<const char*>(span_set_data), span_set_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, span_set_string);

            free(ret);
            free(temp);
            return stored_data;
        }
    );
    if (args.size() == 1) {
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

    auto tgeompoint_from_geom = ScalarFunction(
        "TGEOMPOINT",
        {LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ},
        TGeomPointTypes::TGEOMPOINT(),
        ExecuteTGeomPointFromGeom
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_from_geom);

    auto tgeompoint_from_wkb = ScalarFunction(
        "TGEOMPOINT",
        {TGeomPointTypes::GEOMPOINT(), LogicalType::TIMESTAMP_TZ},
        TGeomPointTypes::TGEOMPOINT(),
        ExecuteTGeomPointFromWKB
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_from_wkb);

    auto tgeompoint_seq = ScalarFunction(
        "tgeompointSeq",
        {LogicalType::LIST(TGeomPointTypes::TGEOMPOINT())},
        TGeomPointTypes::TGEOMPOINT(),
        TemporalFunctions::Tsequence_constructor
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_seq);

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

    auto tgeompoint_duration = ScalarFunction(
        "duration",
        {TGeomPointTypes::TGEOMPOINT(), LogicalType::BOOLEAN},
        LogicalType::INTERVAL,
        TemporalFunctions::Temporal_duration
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_duration);

    auto tgeompoint_trajectory = ScalarFunction(
        "trajectory",
        {TGeomPointTypes::TGEOMPOINT()},
        TGeomPointTypes::GEOMPOINT(),
        ExecuteTGeomPointTrajectory
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_trajectory);

    auto tgeompoint_at_time = ScalarFunction(
        "atTime",
        {TGeomPointTypes::TGEOMPOINT(), SpanTypes::TSTZSPAN()},
        TGeomPointTypes::TGEOMPOINT(),
        ExecuteTGeomPointAtTime
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_at_time);

    auto tgeompoint_at_tstzspanset = ScalarFunction(
        "atTime",
        {TGeomPointTypes::TGEOMPOINT(), SpansetTypes::tstzspanset()},
        TGeomPointTypes::TGEOMPOINT(),
        ExecuteTGeomPointAtTstzspanset
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_at_tstzspanset);

    auto tgeompoint_at_values = ScalarFunction(
        "atValues",
        {TGeomPointTypes::TGEOMPOINT(), TGeomPointTypes::GEOMPOINT()},
        TGeomPointTypes::TGEOMPOINT(),
        ExecuteTGeomPointAtValues
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_at_values);

    auto tgeompoint_start_timestamp = ScalarFunction(
        "startTimestamp",
        {TGeomPointTypes::TGEOMPOINT()},
        LogicalType::TIMESTAMP_TZ,
        ExecuteTGeomPointStartTimestamp
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_start_timestamp);

    auto tgeompoint_value_at_timestamp = ScalarFunction(
        "valueAtTimestamp",
        {TGeomPointTypes::TGEOMPOINT(), LogicalType::TIMESTAMP_TZ},
        TGeomPointTypes::GEOMPOINT(),
        ExecuteTGeomPointValueAtTimestamp
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_value_at_timestamp);

    auto tgeompoint_edwithin = ScalarFunction(
        "eDwithin",
        {TGeomPointTypes::TGEOMPOINT(), TGeomPointTypes::TGEOMPOINT(), LogicalType::DOUBLE},
        LogicalType::BOOLEAN,
        ExecuteTGeomPointEdwithin
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_edwithin);

    auto tgeompoint_tdwithin = ScalarFunction(
        "tDwithin",
        {TGeomPointTypes::TGEOMPOINT(), TGeomPointTypes::TGEOMPOINT(), LogicalType::DOUBLE},
        TemporalTypes::TBOOL(),
        ExecuteTGeomPointTdwithin
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_tdwithin);

    auto tgeompoint_adisjoint = ScalarFunction(
        "aDisjoint",
        {TGeomPointTypes::TGEOMPOINT(), TGeomPointTypes::TGEOMPOINT()},
        LogicalType::BOOLEAN,
        ExecuteTGeomPointAdisjoint
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_adisjoint);

    auto tgeompoint_to_stbox = ScalarFunction(
        "stbox",
        {TGeomPointTypes::TGEOMPOINT()},
        StboxType::STBOX(),
        Tgeompoint_to_stbox
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_to_stbox);

    auto tgeompoint_temporal_overlaps_stbox = ScalarFunction(
        "temporalOverlaps",
        {TGeomPointTypes::TGEOMPOINT(), StboxType::STBOX()},
        LogicalType::BOOLEAN,
        Temporal_overlaps_tgeompoint_stbox
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_temporal_overlaps_stbox);

    auto tgeompoint_temporal_overlaps_tstzspan = ScalarFunction(
        "temporalOverlaps",
        {TGeomPointTypes::TGEOMPOINT(), SpanTypes::TSTZSPAN()},
        LogicalType::BOOLEAN,
        Temporal_overlaps_tgeompoint_tstzspan
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_temporal_overlaps_tstzspan);

    auto tgeompoint_temporal_contains_stbox = ScalarFunction(
        "temporalContains",
        {TGeomPointTypes::TGEOMPOINT(), StboxType::STBOX()},
        LogicalType::BOOLEAN,
        Temporal_contains_tgeompoint_stbox
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_temporal_contains_stbox);

    auto tgeompoint_length = ScalarFunction(
        "length",
        {TGeomPointTypes::TGEOMPOINT()},
        LogicalType::DOUBLE,
        Tgeompoint_length
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_length);

    auto tgeompoint_get_time = ScalarFunction(
        "getTime",
        {TGeomPointTypes::TGEOMPOINT()},
        SpansetTypes::tstzspanset(),
        Tgeompoint_get_time
    );
    ExtensionUtil::RegisterFunction(instance, tgeompoint_get_time);
}

void TGeomPointTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TGEOMPOINT", TGeomPointTypes::TGEOMPOINT());
}

void TGeomPointTypes::RegisterCastFunctions(DatabaseInstance &db) {
    // ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, TGeomPointTypes::TGEOMPOINT(), TgeomPointFunctions::StringToTgeomPoint);
    ExtensionUtil::RegisterCastFunction(db, LogicalType::VARCHAR, TGeomPointTypes::TGEOMPOINT(), TgeomPointFunctions::StringToTgeomPointNew);
    // ExtensionUtil::RegisterCastFunction(db, TGeomPointTypes::TGEOMPOINT(), LogicalType::VARCHAR, TgeomPointFunctions::TgeomPointToString);
    ExtensionUtil::RegisterCastFunction(db, TGeomPointTypes::TGEOMPOINT(), LogicalType::VARCHAR, TgeomPointFunctions::TgeomPointToStringNew);
    ExtensionUtil::RegisterCastFunction(db, TGeomPointTypes::TGEOMPOINT(), StboxType::STBOX(), Tgeompoint_to_stbox_cast);
}

}