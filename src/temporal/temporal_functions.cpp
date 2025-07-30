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

interval_t TemporalHelpers::MeosToDuckDBInterval(MeosInterval *interval) {
    interval_t duckdb_interval;
    duckdb_interval.months = interval->month;
    duckdb_interval.days = interval->day;
    duckdb_interval.micros = interval->time;
    return duckdb_interval;
}

vector<Value> TemporalHelpers::TempArrToArray(Temporal **temparr, int32_t count, LogicalType element_type) {
    vector<Value> values;
    values.reserve(count);

    for (idx_t i = 0; i < count; i++) {
        vector<Value> struct_values;
        struct_values.push_back(Value::BIGINT((uintptr_t)temparr[i]));

        Value val = Value::STRUCT(element_type, struct_values);
        values.push_back(val);
    }
    return values;
}

LogicalType TemporalHelpers::GetTemporalLogicalType(meosType temptype) {
    switch (temptype) {
        case T_TINT:
            return TInt::TIntMake();
        case T_TBOOL:
            return TBool::TBoolMake();
        default:
            throw InternalException("Unsupported temporal type: " + std::to_string(temptype));
    }
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

void TemporalFunctions::TemporalMinValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum ret = temporal_min_value(temp);
        result.SetValue(i, Value::BIGINT((int64_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMaxValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum ret = temporal_max_value(temp);
        result.SetValue(i, Value::BIGINT((int64_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalValueN(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &n_vec = args.data[1];
    value_vec.Flatten(count);
    n_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        int n = n_vec.GetValue(i).GetValue<int32_t>();
        Datum ret;
        bool found = temporal_value_n(temp, n, &ret);
        if (!found) {
            result.SetValue(i, Value());
            continue;
        }
        result.SetValue(i, Value::BIGINT((int64_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMinInstant(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        TInstant *ret = temporal_min_instant(temp);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMaxInstant(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        TInstant *ret = temporal_max_instant(temp);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TInstantTimestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);
    
    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        TInstant *inst = (TInstant*)value;
        ensure_temporal_isof_subtype((Temporal*)inst, TINSTANT);
        timestamp_tz_t ret = (timestamp_tz_t)inst->t;
        result.SetValue(i, Value::TIMESTAMPTZ(ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalDuration(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &boundspan_vec = args.data[1];
    value_vec.Flatten(count);
    boundspan_vec.Flatten(count);

    auto &children = StructVector::GetEntries(value_vec);
    auto &meos_ptr_child = children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        bool boundspan = boundspan_vec.GetValue(i).GetValue<bool>();
        MeosInterval *ret = temporal_duration(temp, boundspan);
        interval_t duckdb_interval = TemporalHelpers::MeosToDuckDBInterval(ret);
        result.SetValue(i, Value::INTERVAL(duckdb_interval));
        free(ret);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TsequenceConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &array_vec = args.data[0];
    array_vec.Flatten(count);
    auto *list_entries = ListVector::GetData(array_vec);
    auto &child_vec = ListVector::GetEntry(array_vec);

    auto &in_children = StructVector::GetEntries(child_vec);
    auto &in_meos_ptr_child = in_children[0];
    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    meosType temptype = TemporalHelpers::GetTemptypeFromAlias(result.GetType().GetAlias().c_str());
    interpType interp = temptype_continuous(temptype) ? LINEAR : STEP;
    bool lower_inc = true;
    bool upper_inc = true;

    for (idx_t i = 0; i < count; i++) {
        auto offset = list_entries[i].offset;
        auto length = list_entries[i].length;

        if (args.size() > 1) {
            auto &interp_child = args.data[1];
            interp_child.Flatten(count);
            auto interp_str = interp_child.GetValue(i).ToString();
            interp = interptype_from_string(interp_str.c_str());
        }
        if (args.size() > 2) {
            auto &lower_inc_child = args.data[2];
            lower_inc = lower_inc_child.GetValue(i).GetValue<bool>();
        }
        if (args.size() > 3) {
            auto &upper_inc_child = args.data[3];
            upper_inc = upper_inc_child.GetValue(i).GetValue<bool>();
        }

        TInstant **instants = (TInstant **)malloc(length * sizeof(TInstant *));
        for (idx_t j = 0; j < length; j++) {
            idx_t child_idx = offset + j;
            uintptr_t value = in_meos_ptr_child->GetValue(child_idx).GetValue<uintptr_t>();
            instants[j] = (TInstant*)value;
        }
        TSequence *seq = tsequence_make((const TInstant **) instants, length,
            lower_inc, upper_inc, interp, true);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)seq));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalToTsequence(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        interpType interp = INTERP_NONE;
        if (args.size() > 1) {
            auto &interp_child = args.data[1];
            interp_child.Flatten(count);
            auto interp_str = interp_child.GetValue(i).ToString();
            interp = interptype_from_string(interp_str.c_str());
        }
        TSequence *ret = temporal_to_tsequence(temp, interp);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TsequencesetConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &array_vec = args.data[0];
    array_vec.Flatten(count);
    auto *list_entries = ListVector::GetData(array_vec);
    auto &child_vec = ListVector::GetEntry(array_vec);

    auto &in_children = StructVector::GetEntries(child_vec);
    auto &in_meos_ptr_child = in_children[0];
    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        auto offset = list_entries[i].offset;
        auto length = list_entries[i].length;
        TSequence **sequences = (TSequence **)malloc(length * sizeof(TSequence *));
        for (idx_t j = 0; j < length; j++) {
            idx_t child_idx = offset + j;
            uintptr_t value = in_meos_ptr_child->GetValue(child_idx).GetValue<uintptr_t>();
            sequences[j] = (TSequence*)value;
        }
        TSequenceSet *seqset = tsequenceset_make((const TSequence **)sequences, length, true);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)seqset));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalToTsequenceset(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        interpType interp = INTERP_NONE;
        if (args.size() > 1) {
            auto &interp_child = args.data[1];
            interp_child.Flatten(count);
            auto interp_str = interp_child.GetValue(i).ToString();
            interp = interptype_from_string(interp_str.c_str());
        }
        TSequenceSet *ret = temporal_to_tsequenceset(temp, interp);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalToTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Span *ret = (Span*)malloc(sizeof(Span));
        temporal_set_tstzspan(temp, ret);
        char *str = span_out(ret, 0);
        result.SetValue(i, Value(str));
        free(str);
        free(ret);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberToSpan(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Span *ret = tnumber_to_span(temp);
        char *str = span_out(ret, 0);
        result.SetValue(i, Value(str));
        free(str);
        free(ret);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalValueset(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        int32_t count;
        Datum *values = temporal_values_p(temp, &count);
        meosType basetype = temptype_basetype((meosType)temp->temptype);
        if (temp->temptype == T_TBOOL) {
            // TODO: handle tbool
            continue;
        }
        Set *ret = set_make_free(values, count, basetype, ORDER_NO);
        char *str = set_out(ret, 0);
        result.SetValue(i, Value(str));
        free(str);
        free(ret);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    value_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        int32_t count;
        const TSequence **sequences = temporal_sequences_p(temp, &count);
        LogicalType element_type = TemporalHelpers::GetTemporalLogicalType((meosType)temp->temptype);
        if (count == 0) {
            result.SetValue(i, Value::LIST(element_type, vector<Value>()));
            continue;
        }
        vector<Value> values = TemporalHelpers::TempArrToArray((Temporal **)sequences, count, element_type);
        Value list_value = Value::LIST(element_type, values);
        result.SetValue(i, list_value);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberShiftValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &shift_vec = args.data[1];
    value_vec.Flatten(count);
    shift_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum shift = shift_vec.GetValue(i).GetValue<Datum>();
        Temporal *ret = tnumber_shift_scale_value(temp, shift, 0, true, false);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberScaleValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &duration_vec = args.data[1];
    value_vec.Flatten(count);
    duration_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum duration = duration_vec.GetValue(i).GetValue<Datum>();
        Temporal *ret = tnumber_shift_scale_value(temp, 0, duration, false, true);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberShiftScaleValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &value_vec = args.data[0];
    auto &shift_vec = args.data[1];
    auto &duration_vec = args.data[2];
    value_vec.Flatten(count);
    shift_vec.Flatten(count);
    duration_vec.Flatten(count);

    auto &in_children = StructVector::GetEntries(value_vec);
    auto &in_meos_ptr_child = in_children[0];

    auto &out_children = StructVector::GetEntries(result);
    auto &out_meos_ptr_child = out_children[0];

    for (idx_t i = 0; i < count; i++) {
        uintptr_t value = in_meos_ptr_child->GetValue(i).GetValue<uintptr_t>();
        Temporal *temp = (Temporal*)value;
        Datum shift = shift_vec.GetValue(i).GetValue<Datum>();
        Datum duration = duration_vec.GetValue(i).GetValue<Datum>();
        Temporal *ret = tnumber_shift_scale_value(temp, shift, duration, true, true);
        out_meos_ptr_child->SetValue(i, Value::UBIGINT((uintptr_t)ret));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

} // namespace duckdb