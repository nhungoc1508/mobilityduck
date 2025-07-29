#include "common.hpp"
#include "temporal/temporal_types.hpp"
#include "temporal/temporal_functions.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>
    #include "temporal/type_inout.h"
}

namespace duckdb {

static const alias_type_struct DUCKDB_ALIAS_TYPE_CATALOG[] = {
    {(char*)"TINT", T_TINT},
    {(char*)"TFLOAT", T_TFLOAT},
    {(char*)"TBOOL", T_TBOOL}
};

meosType TemporalHelpers::GetTemptypeFromAlias(const char *alias) {
    for (size_t i = 0; i < sizeof(DUCKDB_ALIAS_TYPE_CATALOG) / sizeof(DUCKDB_ALIAS_TYPE_CATALOG[0]); i++) {
        if (strcmp(alias, DUCKDB_ALIAS_TYPE_CATALOG[i].alias) == 0) {
            return DUCKDB_ALIAS_TYPE_CATALOG[i].temptype;
        }
    }
    throw InternalException("Unknown alias: " + std::string(alias));
}

bool TemporalFunctions::StringToTemporal(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);

    auto &children = StructVector::GetEntries(result);
    auto &meos_ptr_child = children[0];

    auto &target_type = result.GetType();
    meosType temptype = TemporalHelpers::GetTemptypeFromAlias(target_type.GetAlias().c_str());

    for (idx_t i = 0; i < count; i++) {
        auto input_str = source.GetValue(i).ToString();
        Temporal *temp = temporal_in(input_str.c_str(), temptype);
        meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)temp));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
    return true;
}

bool TemporalFunctions::TemporalToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);

    auto &children = StructVector::GetEntries(source);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t meos_ptr = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)meos_ptr;
        char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
        result.SetValue(i, Value(str));
        free(str);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
    return true;
}

void TemporalFunctions::TInstantConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &ts_vec = args.data[1];

    value_vec.Flatten(count);
    ts_vec.Flatten(count);

    auto &ret_children = StructVector::GetEntries(result);
    auto &meos_ptr_child = ret_children[0];

    for (idx_t i = 0; i < count; i++) {
        auto value = value_vec.GetValue(i).GetValue<uintptr_t>();
        auto ts = ts_vec.GetValue(i).GetValue<timestamp_tz_t>();
        meosType temptype = TemporalHelpers::GetTemptypeFromAlias(result.GetType().GetAlias().c_str());
        TInstant *inst = tinstant_make((Datum)value, temptype, (TimestampTz)ts.value);
        meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)inst));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        tempSubtype subtype = (tempSubtype)temp->subtype;
        const char *str = tempsubtype_name(subtype);
        result.SetValue(i, Value(str));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalInterp(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];
    
    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        const char *str = temporal_interp(temp);
        result.SetValue(i, Value(str));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TInstantValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        TInstant *inst = (TInstant*)value;
        Datum ret = tinstant_value(inst);
        result.SetValue(i, Value::BIGINT((int64_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum ret = temporal_start_value(temp);
        result.SetValue(i, Value::BIGINT((int64_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum ret = temporal_end_value(temp);
        result.SetValue(i, Value::BIGINT((int64_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

} // namespace duckdb