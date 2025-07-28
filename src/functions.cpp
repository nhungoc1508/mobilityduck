#include "common.hpp"
#include "types.hpp"
#include "functions.hpp"
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

struct TemporalHelper {
    static TInstant* MakeInstant(const unique_ptr<Vector> &instant_child, idx_t i) {
        auto &fields = StructVector::GetEntries(*instant_child);
        // [0] - value, [1] - temptype, [2] - t
        int64_t value = fields[0]->GetValue(i).GetValue<int64_t>();
        uint8_t temptype = fields[1]->GetValue(i).GetValue<uint8_t>();
        TimestampTz t = fields[2]->GetValue(i).GetValue<timestamp_tz_t>().value;
        TInstant *inst = tinstant_make((Datum)value, (meosType)temptype, t);
        return inst;
    }

    static TSequence* MakeSequence(const unique_ptr<Vector> &sequence_child, idx_t i) {
        auto &seq_fields = StructVector::GetEntries(*sequence_child);
        // [0] - instants, [1] - interp, [2] - lower_inc, [3] - upper_inc
        auto &instants_vector = *seq_fields[0];
        auto &instants_list = ListVector::GetEntry(instants_vector);
        auto &instant_fields = StructVector::GetEntries(instants_list);
        // [0] - value, [1] - temptype, [2] - t

        vector<TInstant*> instants;
        auto *list_entries = ListVector::GetData(instants_vector);
        auto list_offset = list_entries[i].offset;
        auto list_length = list_entries[i].length;
        for (idx_t j = 0; j < list_length; j++) {
            idx_t child_idx = list_offset + j;
            int64_t value = instant_fields[0]->GetValue(child_idx).GetValue<int64_t>();
            uint8_t temptype = instant_fields[1]->GetValue(child_idx).GetValue<uint8_t>();
            TimestampTz t = instant_fields[2]->GetValue(child_idx).GetValue<timestamp_tz_t>().value;
            instants.push_back(tinstant_make((Datum)value, (meosType)temptype, t));
        }
        uint8_t interp = seq_fields[1]->GetValue(i).GetValue<uint8_t>();
        bool lower_inc = seq_fields[2]->GetValue(i).GetValue<bool>();
        bool upper_inc = seq_fields[3]->GetValue(i).GetValue<bool>();
        TSequence *seq = tsequence_make((const TInstant**)instants.data(), instants.size(),
            lower_inc, upper_inc,(interpType)interp, true);
        for (auto inst : instants) free(inst);
        return seq;
    }

    static TSequenceSet* MakeSequenceSet(const unique_ptr<Vector> &sequenceset_child, idx_t i) {
        auto &seqset_fields = StructVector::GetEntries(*sequenceset_child);
        // [0] - sequences
        auto &seqs_vector = *seqset_fields[0];
        auto &seqs_list = ListVector::GetEntry(seqs_vector);
        auto &seq_fields = StructVector::GetEntries(seqs_list);

        vector<TSequence*> sequences;
        auto *seq_list_entries = ListVector::GetData(seqs_vector);
        auto seq_list_offset = seq_list_entries[i].offset;
        auto seq_list_length = seq_list_entries[i].length;
        for (idx_t j = 0; j < seq_list_length; j++) {
            idx_t seq_idx = seq_list_offset + j;
            auto &instants_vector = *seq_fields[0];
            auto &instants_list = ListVector::GetEntry(instants_vector);
            auto &instant_fields = StructVector::GetEntries(instants_list);
            // [0] - value, [1] - temptype, [2] - t

            vector<TInstant*> instants;
            auto *list_entries = ListVector::GetData(instants_vector);
            auto list_offset = list_entries[seq_idx].offset;
            auto list_length = list_entries[seq_idx].length;
            for (idx_t k = 0; k < list_length; k++) {
                idx_t child_idx = list_offset + k;
                int64_t value = instant_fields[0]->GetValue(child_idx).GetValue<int64_t>();
                uint8_t temptype = instant_fields[1]->GetValue(child_idx).GetValue<uint8_t>();
                TimestampTz t = instant_fields[2]->GetValue(child_idx).GetValue<timestamp_tz_t>().value;
                instants.push_back(tinstant_make((Datum)value, (meosType)temptype, t));
            }
            uint8_t interp = seq_fields[1]->GetValue(seq_idx).GetValue<uint8_t>();
            bool lower_inc = seq_fields[2]->GetValue(seq_idx).GetValue<bool>();
            bool upper_inc = seq_fields[3]->GetValue(seq_idx).GetValue<bool>();
            TSequence* seq = tsequence_make((const TInstant**)instants.data(), instants.size(),
                lower_inc, upper_inc, (interpType)interp, true);
            sequences.push_back(seq);
            for (auto inst : instants) free(inst);
        }
        TSequenceSet *seqset = tsequenceset_make((const TSequence**)sequences.data(), sequences.size(), true);
        for (auto seq : sequences) free(seq);
        return seqset;
    }
};

struct TIntFunctions {
    static void ExecuteTintMake(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &value_vec = args.data[0];
        auto &t_vec = args.data[1];

        value_vec.Flatten(count);
        t_vec.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            auto value = value_vec.GetValue(i).GetValue<int64_t>();
            auto t = t_vec.GetValue(i).GetValue<timestamp_tz_t>();
            TInstant *inst = tinstant_make((Datum)value, T_TINT, (TimestampTz)t.value);

            Value instant_struct = Value::STRUCT({
                {"value", Value::BIGINT((int64_t)inst->value)},
                {"temptype", Value::UTINYINT(inst->temptype)},
                {"t", Value::TIMESTAMPTZ(timestamp_tz_t(inst->t))}
            });

            result.SetValue(i, Value::STRUCT({
                {"subtype", Value::UTINYINT(tempSubtype::TINSTANT)},
                {"instant", instant_struct},
                {"sequence", Value()},
                {"sequenceset", Value()}
            }));
            free(inst);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static bool ExecuteTIntInFromString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
        source.Flatten(count);

        auto &children = StructVector::GetEntries(result);
        auto &subtype_child = children[0];
        auto &instant_child = children[1];
        auto &sequence_child = children[2];
        auto &sequenceset_child = children[3];

        for (idx_t i = 0; i < count; i++) {
            auto input_str = source.GetValue(i).ToString();
            Temporal *temp = temporal_in(input_str.c_str(), T_TINT);
            Value subtype_val, instant_val, sequence_val, sequenceset_val;
            switch (temp->subtype) {
                case tempSubtype::TINSTANT:
                {
                    TInstant *inst = (TInstant*)temp;
                    subtype_val = Value::UTINYINT(tempSubtype::TINSTANT);
                    instant_val = Value::STRUCT({
                        {"value", Value::BIGINT((int64_t)inst->value)},
                        {"temptype", Value::UTINYINT(inst->temptype)},
                        {"t", Value::TIMESTAMPTZ(timestamp_tz_t(inst->t))}
                    });
                    sequence_val = Value();
                    sequenceset_val = Value();
                    break;
                }
                case tempSubtype::TSEQUENCE:
                {
                    TSequence *seq = (TSequence*)temp;
                    subtype_val = Value::UTINYINT(tempSubtype::TSEQUENCE);
                    vector<Value> instants;
                    for (idx_t j = 0; j < seq->count; j++) {
                        const TInstant *inst = TSEQUENCE_INST_N(seq, j);
                        instants.push_back(Value::STRUCT({
                            {"value", Value::BIGINT((int64_t)inst->value)},
                            {"temptype", Value::UTINYINT(inst->temptype)},
                            {"t", Value::TIMESTAMPTZ(timestamp_tz_t(inst->t))}
                        }));
                    }
                    instant_val = Value();
                    sequence_val = Value::STRUCT({
                        {"instants", Value::LIST(LogicalType::STRUCT({
                            {"value", LogicalType::BIGINT},
                            {"temptype", LogicalType::UTINYINT},
                            {"t", LogicalType::TIMESTAMP_TZ}
                        }), instants)},
                        {"interp", Value::UTINYINT(MEOS_FLAGS_GET_INTERP(seq->flags))},
                        {"lower_inc", Value::BOOLEAN(seq->period.lower_inc)},
                        {"upper_inc", Value::BOOLEAN(seq->period.upper_inc)}
                    });
                    sequenceset_val = Value();
                    break;
                }
                case tempSubtype::TSEQUENCESET:
                {
                    TSequenceSet *seqset = (TSequenceSet*)temp;
                    subtype_val = Value::UTINYINT(tempSubtype::TSEQUENCESET);
                    vector<Value> sequences;
                    for (idx_t k = 0; k < seqset->count; k++) {
                        const TSequence *seq = TSEQUENCESET_SEQ_N(seqset, k);
                        vector<Value> instants;
                        for (idx_t j = 0; j < seq->count; j++) {
                            const TInstant *inst = TSEQUENCE_INST_N(seq, j);
                            instants.push_back(Value::STRUCT({
                                {"value", Value::BIGINT((int64_t)inst->value)},
                                {"temptype", Value::UTINYINT(inst->temptype)},
                                {"t", Value::TIMESTAMPTZ(timestamp_tz_t(inst->t))}
                            }));
                        }
                        sequences.push_back(Value::STRUCT({
                            {"instants", Value::LIST(LogicalType::STRUCT({
                                {"value", LogicalType::BIGINT},
                                {"temptype", LogicalType::UTINYINT},
                                {"t", LogicalType::TIMESTAMP_TZ}
                            }), instants)},
                            {"interp", Value::UTINYINT(MEOS_FLAGS_GET_INTERP(seq->flags))},
                            {"lower_inc", Value::BOOLEAN(seq->period.lower_inc)},
                            {"upper_inc", Value::BOOLEAN(seq->period.upper_inc)}
                        }));
                    }
                    instant_val = Value();
                    sequence_val = Value();
                    sequenceset_val = Value::STRUCT({
                        {"sequences", Value::LIST(GeoTypes::TSequenceType(), sequences)}
                    });
                    break;
                }
                default:
                    throw InternalException("[Input] Unknown temporal subtype");
            }

            result.SetValue(i, Value::STRUCT({
                {"subtype", subtype_val},
                {"instant", instant_val},
                {"sequence", sequence_val},
                {"sequenceset", sequenceset_val}
            }));
            free(temp);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
        return true;
    }

    static bool ExecuteStringFromTInt(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
        source.Flatten(count);

        auto &children = StructVector::GetEntries(source);
        // [0] - subtype, [1] - instant, [2] - sequence, [3] - sequenceset
        auto &subtype_child = children[0];
        auto &instant_child = children[1];
        auto &sequence_child = children[2];
        auto &sequenceset_child = children[3];

        for (idx_t i = 0; i < count; i++) {
            uint8_t subtype = subtype_child->GetValue(i).GetValue<uint8_t>();
            char *str = nullptr;

            switch (subtype) {
                case tempSubtype::TINSTANT:
                {
                    TInstant *inst = TemporalHelper::MakeInstant(instant_child, i);
                    str = temporal_out((Temporal*)inst, OUT_DEFAULT_DECIMAL_DIGITS);
                    free(inst);
                    break;
                }
                case tempSubtype::TSEQUENCE:
                {
                    TSequence *seq = TemporalHelper::MakeSequence(sequence_child, i);
                    str = temporal_out((Temporal*)seq, OUT_DEFAULT_DECIMAL_DIGITS);
                    free(seq);
                    break;
                }
                case tempSubtype::TSEQUENCESET:
                {
                    TSequenceSet *seqset = TemporalHelper::MakeSequenceSet(sequenceset_child, i);
                    str = temporal_out((Temporal*)seqset, OUT_DEFAULT_DECIMAL_DIGITS);
                    free(seqset);
                    break;
                }
                default:
                    throw InternalException("[Output] Unknown temporal subtype");
            }

            result.SetValue(i, Value(str));
            free(str);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
        return true;
    }

    static void ExecuteTempSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tint_vector = args.data[0];
        tint_vector.Flatten(count);

        for (idx_t i = 0; i < count; i++) {
            auto &children = StructVector::GetEntries(tint_vector);
            // [0] - subtype, [1] - instant, [2] - sequence, [3] - sequenceset
            auto &subtype_child = children[0];
            tempSubtype subtype = (tempSubtype)subtype_child->GetValue(i).GetValue<uint8_t>();
            if (!temptype_subtype(subtype)) {
                throw InternalException("Unknown temporal subtype: %d", subtype);
            }
            const char *str = tempsubtype_name(subtype);
            result.SetValue(i, Value(str));
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static void ExecuteInterp(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tint_vector = args.data[0];
        tint_vector.Flatten(count);

        auto &children = StructVector::GetEntries(tint_vector);
        auto &subtype_child = children[0];
        auto &instant_child = children[1];
        auto &sequence_child = children[2];
        auto &sequenceset_child = children[3];

        for (idx_t i = 0; i < count; i++) {
            tempSubtype subtype = (tempSubtype)subtype_child->GetValue(i).GetValue<uint8_t>();
            Temporal *temp = nullptr;
            switch (subtype) {
                case tempSubtype::TINSTANT:
                {
                    TInstant *inst = TemporalHelper::MakeInstant(instant_child, i);
                    temp = (Temporal*)inst;
                    break;
                }
                case tempSubtype::TSEQUENCE:
                {
                    TSequence *seq = TemporalHelper::MakeSequence(sequence_child, i);
                    temp = (Temporal*)seq;
                    break;
                }
                case tempSubtype::TSEQUENCESET:
                {
                    TSequenceSet *seqset = TemporalHelper::MakeSequenceSet(sequenceset_child, i);
                    temp = (Temporal*)seqset;
                    break;
                }
                default:
                    throw InternalException("Unknown temporal subtype: %d", subtype);
            }
            if (!temp) {
                result.SetValue(i, Value());
                continue;
            }
            const char *str = temporal_interp(temp);
            result.SetValue(i, Value(str));
            free(temp);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static void ExecuteStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tint_vector = args.data[0];
        tint_vector.Flatten(count);

        auto &children = StructVector::GetEntries(tint_vector);
        auto &subtype_child = children[0];
        auto &instant_child = children[1];
        auto &sequence_child = children[2];
        auto &sequenceset_child = children[3];

        for (idx_t i = 0; i < count; i++) {
            tempSubtype subtype = (tempSubtype)subtype_child->GetValue(i).GetValue<uint8_t>();
            Temporal *temp = nullptr;
            switch (subtype) {
                case tempSubtype::TINSTANT:
                {
                    TInstant *inst = TemporalHelper::MakeInstant(instant_child, i);
                    temp = (Temporal*)inst;
                    break;
                }
                case tempSubtype::TSEQUENCE:
                {
                    TSequence *seq = TemporalHelper::MakeSequence(sequence_child, i);
                    temp = (Temporal*)seq;
                    break;
                }
                case tempSubtype::TSEQUENCESET:
                {
                    TSequenceSet *seqset = TemporalHelper::MakeSequenceSet(sequenceset_child, i);
                    temp = (Temporal*)seqset;
                    break;
                }
                default:
                    throw InternalException("Unknown temporal subtype: %d", subtype);
            }
            if (!temp) {
                result.SetValue(i, Value());
                continue;
            }
            Datum val = temporal_start_value(temp);
            result.SetValue(i, Value::BIGINT((int64_t)val));
            free(temp);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static void ExecuteEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tint_vector = args.data[0];
        tint_vector.Flatten(count);

        auto &children = StructVector::GetEntries(tint_vector);
        auto &subtype_child = children[0];
        auto &instant_child = children[1];
        auto &sequence_child = children[2];
        auto &sequenceset_child = children[3];

        for (idx_t i = 0; i < count; i++) {
            tempSubtype subtype = (tempSubtype)subtype_child->GetValue(i).GetValue<uint8_t>();
            Temporal *temp = nullptr;
            switch (subtype) {
                case tempSubtype::TINSTANT:
                {
                    TInstant *inst = TemporalHelper::MakeInstant(instant_child, i);
                    temp = (Temporal*)inst;
                    break;
                }
                case tempSubtype::TSEQUENCE:
                {
                    TSequence *seq = TemporalHelper::MakeSequence(sequence_child, i);
                    temp = (Temporal*)seq;
                    break;
                }
                case tempSubtype::TSEQUENCESET:
                {
                    TSequenceSet *seqset = TemporalHelper::MakeSequenceSet(sequenceset_child, i);
                    temp = (Temporal*)seqset;
                    break;
                }
                default:
                    throw InternalException("Unknown temporal subtype: %d", subtype);
            }
            if (!temp) {
                result.SetValue(i, Value());
                continue;
            }
            Datum val = temporal_end_value(temp);
            result.SetValue(i, Value::BIGINT((int64_t)val));
            free(temp);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static void ExecuteTIntSeq(DataChunk &args, ExpressionState &state, Vector &result) {
        auto &array_vector = args.data[0];
        array_vector.Flatten(args.size());

        for (idx_t row = 0; row < args.size(); row++) {
            auto *list_entries = ListVector::GetData(array_vector);
            auto offset = list_entries[row].offset;
            auto length = list_entries[row].length;
            auto &child_vector = ListVector::GetEntry(array_vector);
            auto &children = StructVector::GetEntries(child_vector);

            // [0] - subtype, [1] - instant, [2] - sequence, [3] - sequenceset
            auto &subtype_child = children[0];
            auto &instant_child = children[1];
            auto &sequence_child = children[2];
            auto &sequenceset_child = children[3];
            
            vector<Value> instant_structs;
            TInstant **instants = (TInstant **)malloc(length * sizeof(TInstant *));
            for (idx_t i = 0; i < length; i++) {
                idx_t child_idx = offset + i;
                auto &instant_fields = StructVector::GetEntries(*instant_child);
                // [0] - value, [1] - temptype, [2] - t
                int64_t value = instant_fields[0]->GetValue(child_idx).GetValue<int64_t>();
                uint8_t temptype = instant_fields[1]->GetValue(child_idx).GetValue<uint8_t>();
                TimestampTz t = instant_fields[2]->GetValue(child_idx).GetValue<timestamp_tz_t>().value;
                instants[i] = tinstant_make((Datum)value, (meosType)temptype, (TimestampTz)t);
                instant_structs.push_back(Value::STRUCT({
                    {"value", Value::BIGINT(value)},
                    {"temptype", Value::UTINYINT(temptype)},
                    {"t", Value::TIMESTAMPTZ(timestamp_tz_t(t))}
                }));
            }
            meosType temptype = meosType::T_TINT;
            interpType interp = temptype_continuous(temptype) ? LINEAR : STEP;
            interp = interpType::DISCRETE; // TODO: hard coded for now, should handle input string
            bool lower_inc = true; // TODO: hard coded for now
            bool upper_inc = true; // TODO: hard coded for now
            TSequence *seq = tsequence_make((const TInstant **) instants, length,
                lower_inc, upper_inc, interp, true);

            // Build the sequence STRUCT
            Value sequence_struct = Value::STRUCT({
                {"instants", Value::LIST(LogicalType::STRUCT({
                    {"value", LogicalType::BIGINT},
                    {"temptype", LogicalType::UTINYINT},
                    {"t", LogicalType::TIMESTAMP_TZ}
                }), instant_structs)},
                {"interp", Value::UTINYINT((uint8_t)interp)},
                {"lower_inc", Value::BOOLEAN(lower_inc)},
                {"upper_inc", Value::BOOLEAN(upper_inc)}
            });

            // Set the result as a TINT STRUCT (only sequence set)
            result.SetValue(row, Value::STRUCT({
                {"subtype", Value::UTINYINT(tempSubtype::TSEQUENCE)},
                {"instant", Value()},
                {"sequence", sequence_struct},
                {"sequenceset", Value()}
            }));

            free(instants);
            free(seq);
        }
    }
};

struct TInstantFunctions {
    static void ExecuteTInstantMake(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &value_vec = args.data[0];
        auto &temptype_vec = args.data[1];
        auto &t_vec = args.data[2];

        value_vec.Flatten(count);
        temptype_vec.Flatten(count);
        t_vec.Flatten(count);

        auto &children = StructVector::GetEntries(result);
        auto &value_child = children[0];
        auto &temptype_child = children[1];
        auto &t_child = children[2];

        for (idx_t i = 0; i < count; i++) {
            auto value = value_vec.GetValue(i).GetValue<int64_t>();
            auto temptype = temptype_vec.GetValue(i).GetValue<uint8_t>();
            auto t = t_vec.GetValue(i).GetValue<timestamp_tz_t>();

            TInstant *inst = tinstant_make((Datum)value, (meosType)temptype, (TimestampTz)t.value);
            value_child->SetValue(i, Value::BIGINT((int64_t)inst->value));
            temptype_child->SetValue(i, Value::UTINYINT(inst->temptype));
            t_child->SetValue(i, Value::TIMESTAMPTZ(timestamp_tz_t(inst->t)));
            free(inst);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static void ExecuteTInstantValue(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tinstant_vec = args.data[0];
        tinstant_vec.Flatten(count);

        auto &children = StructVector::GetEntries(tinstant_vec);
        auto &instant_child = children[1];

        for (idx_t i = 0; i < count; i++) {
            TInstant *inst = TemporalHelper::MakeInstant(instant_child, i);
            Datum val = tinstant_value(inst);
            result.SetValue(i, Value::BIGINT((int64_t)val));
            free(inst);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static void ExecuteTInstantToString(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &tinstant_vec = args.data[0];
        tinstant_vec.Flatten(count);

        auto &children = StructVector::GetEntries(tinstant_vec);
        auto &instant_child = children[1];

        for (idx_t i = 0; i < count; i++) {
            TInstant *inst = TemporalHelper::MakeInstant(instant_child, i);
            int maxdd = 15;
            char *str = temporal_out((Temporal*)inst, maxdd);
            result.SetValue(i, Value(str));
            free(str);
            free(inst);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }
};

void GeoFunctions::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto tinstant_make_function = ScalarFunction(
        "TINSTANT_MAKE",
        {LogicalType::BIGINT, LogicalType::UTINYINT, LogicalType::TIMESTAMP_TZ}, // arguments
        GeoTypes::TINSTANT(),
        TInstantFunctions::ExecuteTInstantMake);
    ExtensionUtil::RegisterFunction(instance, tinstant_make_function);

    auto tinstant_value_function = ScalarFunction(
        "TINSTANT_VALUE",
        {GeoTypes::TINSTANT()},
        LogicalType::BIGINT,
        TInstantFunctions::ExecuteTInstantValue);
    ExtensionUtil::RegisterFunction(instance, tinstant_value_function);

    auto tinstant_to_string_function = ScalarFunction(
        "TINSTANT_TO_STRING",
        {GeoTypes::TINSTANT()},
        LogicalType::VARCHAR,
        TInstantFunctions::ExecuteTInstantToString);
    ExtensionUtil::RegisterFunction(instance, tinstant_to_string_function);

    auto tint_make_function = ScalarFunction(
        "TINT",
        {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ},
        GeoTypes::TINT(),
        TIntFunctions::ExecuteTintMake);
    ExtensionUtil::RegisterFunction(instance, tint_make_function);

    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        GeoTypes::TINT(),
        TIntFunctions::ExecuteTIntInFromString
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        GeoTypes::TINT(),
        LogicalType::VARCHAR,
        TIntFunctions::ExecuteStringFromTInt
    );

    auto tint_value_function = ScalarFunction(
        "getValue",
        {GeoTypes::TINT()},
        LogicalType::BIGINT,
        TInstantFunctions::ExecuteTInstantValue);
    ExtensionUtil::RegisterFunction(instance, tint_value_function);

    auto tint_to_string_function = ScalarFunction(
        "asText",
        {GeoTypes::TINT()},
        LogicalType::VARCHAR,
        TInstantFunctions::ExecuteTInstantToString);
    ExtensionUtil::RegisterFunction(instance, tint_to_string_function);

    auto tint_tempsubtype_function = ScalarFunction(
        "tempSubtype",
        {GeoTypes::TINT()},
        LogicalType::VARCHAR,
        TIntFunctions::ExecuteTempSubtype);
    ExtensionUtil::RegisterFunction(instance, tint_tempsubtype_function);

    auto tint_interp_function = ScalarFunction(
        "interp",
        {GeoTypes::TINT()},
        LogicalType::VARCHAR,
        TIntFunctions::ExecuteInterp);
    ExtensionUtil::RegisterFunction(instance, tint_interp_function);

    auto tint_startvalue_function = ScalarFunction(
        "startValue",
        {GeoTypes::TINT()},
        LogicalType::BIGINT,
        TIntFunctions::ExecuteStartValue);
    ExtensionUtil::RegisterFunction(instance, tint_startvalue_function);

    auto tint_endvalue_function = ScalarFunction(
        "endValue",
        {GeoTypes::TINT()},
        LogicalType::BIGINT,
        TIntFunctions::ExecuteEndValue);
    ExtensionUtil::RegisterFunction(instance, tint_endvalue_function);

    auto tint_seq_function = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(GeoTypes::TINT())},
        GeoTypes::TINT(),
        TIntFunctions::ExecuteTIntSeq);
    ExtensionUtil::RegisterFunction(instance, tint_seq_function);
}

} // namespace duckdb