#include "common.hpp"
#include "temporal/tint2.hpp"
#include "temporal/temporal_functions.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/extension_type_info.hpp"

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_internal.h>
    #include "temporal/type_inout.h"
}

namespace duckdb {

LogicalType TInt2::TInt2Make() {
    LogicalType type(LogicalTypeId::VARCHAR);
    type.SetAlias("TINT2");
    return type;
}

void TInt2::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINT2", TInt2::TInt2Make());
}

inline bool TInt2::StringToTemporal(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    for (idx_t i = 0; i < count; i++) {
        auto input_str = source.GetValue(i).ToString();
        Temporal *temp = temporal_in(input_str.c_str(), T_TINT);
        char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
        result.SetValue(i, Value(str));
        free(str);  // Use free for MEOS-allocated memory
        free(temp); // Free the temporal object
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
    return true;
}

inline bool TInt2::TemporalToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    for (idx_t i = 0; i < count; i++) {
        auto temp_str = source.GetValue(i).ToString();
        Temporal *temp = temporal_in(temp_str.c_str(), T_TINT);
        char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
        result.SetValue(i, Value(str));
        free(str);  // Use free for MEOS-allocated memory
        free(temp); // Free the temporal object
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
    return true;
}

void TInt2::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TInt2::TInt2Make(),
        TInt2::StringToTemporal
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TInt2::TInt2Make(),
        LogicalType::VARCHAR,
        TInt2::TemporalToString
    );
}

inline void TInt2::TInstantConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &ts_vec = args.data[1];
    value_vec.Flatten(count);
    ts_vec.Flatten(count);

    for (idx_t i = 0; i < count; i++) {
        auto value = value_vec.GetValue(i).GetValue<uintptr_t>();
        auto ts = ts_vec.GetValue(i).GetValue<timestamp_tz_t>();
        meosType temptype = T_TINT;
        TInstant *inst = tinstant_make((Datum)value, temptype, (TimestampTz)ts.value);
        char *str = temporal_out((Temporal *)inst, OUT_DEFAULT_DECIMAL_DIGITS);
        result.SetValue(i, Value(str));
        free(str);  // Use free for MEOS-allocated memory
        free(inst); // Free the instant object
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void TInt2::TemporalSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    for (idx_t i = 0; i < count; i++) {
        auto temp_str = value_vec.GetValue(i).ToString();
        Temporal *temp = temporal_in(temp_str.c_str(), T_TINT);
        tempSubtype subtype = (tempSubtype)temp->subtype;
        const char *str = tempsubtype_name(subtype);
        result.SetValue(i, Value(str));
        free(temp); // Free the temporal object
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void TInt2::TInstantValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    for (idx_t i = 0; i < count; i++) {
        auto temp_str = value_vec.GetValue(i).ToString();
        Temporal *temp = temporal_in(temp_str.c_str(), T_TINT);
        TInstant *inst = (TInstant*)temp;
        Datum ret = tinstant_value(inst);
        result.SetValue(i, Value::BIGINT((int64_t)ret));
        free(temp); // Free the temporal object
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void TInt2::TInstantTimestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);
    for (idx_t i = 0; i < count; i++) {
        auto temp_str = value_vec.GetValue(i).ToString();
        Temporal *temp = temporal_in(temp_str.c_str(), T_TINT);
        TInstant *inst = (TInstant*)temp;
        ensure_temporal_isof_subtype((Temporal*)inst, TINSTANT);
        timestamp_tz_t ret = (timestamp_tz_t)inst->t;
        result.SetValue(i, Value::TIMESTAMPTZ(ret));
        free(temp); // Free the temporal object
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

vector<Value> TInt2::TempArrToArray(Temporal **temparr, int32_t count, LogicalType element_type) {
    vector<Value> values;
    values.reserve(count);

    for (idx_t i = 0; i < count; i++) {
        Temporal *entry = temparr[i];
        char *entry_str = temporal_out(entry, OUT_DEFAULT_DECIMAL_DIGITS);
        printf("i: %ld, entry_str: %s\n", i, entry_str);
        
        // Create a simple string value instead of struct
        Value val = Value(entry_str);
        values.push_back(val);
        
        free(entry_str); // Use free for MEOS-allocated memory
    }
    return values;
}

inline void TInt2::TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    for (idx_t i = 0; i < count; i++) {
        auto temp_str = value_vec.GetValue(i).ToString();
        Temporal *temp = temporal_in(temp_str.c_str(), T_TINT);
        
        int32_t seq_count;
        const TSequence **sequences = temporal_sequences_p(temp, &seq_count);
        
        if (seq_count == 0) {
            result.SetValue(i, Value::LIST(TInt2::TInt2Make(), vector<Value>()));
            free(temp); // Free the temporal object
            continue;
        }
        
        // Convert sequences to string values
        vector<Value> values;
        values.reserve(seq_count);
        
        for (int32_t j = 0; j < seq_count; j++) {
            const TSequence *seq = sequences[j];
            char *seq_str = temporal_out((Temporal*)seq, OUT_DEFAULT_DECIMAL_DIGITS);
            values.push_back(Value(seq_str));
            free(seq_str); // Use free for MEOS-allocated memory
        }
        
        Value list_value = Value::LIST(TInt2::TInt2Make(), values);
        result.SetValue(i, list_value);
        
        free(temp); // Free the temporal object
        // Note: sequences array is managed by MEOS, don't free it
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TInt2::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto constructor = ScalarFunction(
        "tint2",
        {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ},
        TInt2::TInt2Make(),
        TInt2::TInstantConstructor
    );
    ExtensionUtil::RegisterFunction(instance, constructor);

    auto tempsubtype = ScalarFunction(
        "tempSubtype",
        {TInt2::TInt2Make()},
        LogicalType::VARCHAR,
        TInt2::TemporalSubtype
    );
    ExtensionUtil::RegisterFunction(instance, tempsubtype);

    auto get_value = ScalarFunction(
        "getValue",
        {TInt2::TInt2Make()},
        LogicalType::BIGINT,
        TInt2::TInstantValue
    );
    ExtensionUtil::RegisterFunction(instance, get_value);

    auto instant_timestamptz = ScalarFunction(
        "getTimestamp",
        {TInt2::TInt2Make()},
        LogicalType::TIMESTAMP_TZ,
        TInt2::TInstantTimestamptz
    );
    ExtensionUtil::RegisterFunction(instance, instant_timestamptz);

    auto sequences = ScalarFunction(
        "sequences",
        {TInt2::TInt2Make()},
        LogicalType::LIST(TInt2::TInt2Make()),
        TInt2::TemporalSequences
    );
    ExtensionUtil::RegisterFunction(instance, sequences);
}

} // namespace duckdb