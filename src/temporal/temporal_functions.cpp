#include "meos_wrapper_simple.hpp"
#include "common.hpp"
#include "temporal/temporal_types.hpp"
#include "temporal/temporal_functions.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "meos_wrapper_simple.hpp"

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
            return TInt::TINT();
        case T_TBOOL:
            // return TBool::TBoolMake();
        default:
            throw InternalException("Unsupported temporal type: " + std::to_string(temptype));
    }
}

timestamp_tz_t TemporalHelpers::DuckDBToMeosTimestamp(timestamp_tz_t duckdb_ts) {
    timestamp_tz_t meos_ts;
    meos_ts.value = duckdb_ts.value - TIMESTAMP_ADAPT_GAP_MS;
    return meos_ts;
}

timestamp_tz_t TemporalHelpers::MeosToDuckDBTimestamp(timestamp_tz_t meos_ts) {
    timestamp_tz_t duckdb_ts;
    duckdb_ts.value = meos_ts.value + TIMESTAMP_ADAPT_GAP_MS;
    return duckdb_ts;
}

bool TemporalFunctions::StringToTemporal(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    auto &target_type = result.GetType();
    meosType temptype = TemporalHelpers::GetTemptypeFromAlias(target_type.GetAlias().c_str());
    bool success = true;
    try {
        UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
            source, result, count,
            [&](string_t input, ValidityMask &mask, idx_t idx) {
                if (input.GetSize() == 0) {
                    return string_t();
                }
                std::string input_str(input.GetDataUnsafe(), input.GetSize());
                Temporal *temp = temporal_in(input_str.c_str(), temptype);
                if (!temp) {
                    throw InternalException("Failure in StringToTemporal: unable to cast string to temporal");
                    success = false;
                    return string_t();
                }
                // size_t temp_size = VARSIZE_ANY_EXHDR(temp) + VARHDRSZ;
                size_t temp_size = temporal_mem_size(temp);
                char *temp_data = (char*)malloc(temp_size);
                memcpy(temp_data, temp, temp_size);
                return string_t(temp_data, temp_size);
            }
        );
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

bool TemporalFunctions::TemporalToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalToString: unable to cast string to temporal");
                success = false;
                return string_t();
            }
            char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
            return string_t(str);
        }
    );
    return success;
}

void TemporalFunctions::TInstantConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<int64_t, timestamp_tz_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t value, timestamp_tz_t ts) {
            meosType temptype = TemporalHelpers::GetTemptypeFromAlias(result.GetType().GetAlias().c_str());
            timestamp_tz_t meos_ts = TemporalHelpers::DuckDBToMeosTimestamp(ts);
            TInstant *inst = tinstant_make((Datum)value, temptype, (TimestampTz)meos_ts.value);
            Temporal *temp = (Temporal*)inst;
            // size_t temp_size = VARSIZE_ANY_EXHDR(temp) + VARHDRSZ;
            size_t temp_size = temporal_mem_size(temp);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, temp, temp_size);
            return string_t(temp_data, temp_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalSubtype: unable to cast string to temporal");
                return string_t();
            }
            tempSubtype subtype = (tempSubtype)temp->subtype;
            const char *str = tempsubtype_name(subtype);
            free(temp);
            return string_t(str);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalInterp(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalSubtype: unable to cast string to temporal");
                return string_t();
            }
            const char *str = temporal_interp(temp);
            free(temp);
            return string_t(str);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TInstantValue(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TInstantValue: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret = tinstant_value((TInstant*)temp);
            free(temp);
            return (int64_t)ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalStartValue: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret = temporal_start_value(temp);
            free(temp);
            return (int64_t)ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalEndValue: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret = temporal_end_value(temp);
            free(temp);
            return (int64_t)ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMinValue(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalEndValue: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret = temporal_min_value(temp);
            free(temp);
            return (int64_t)ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMaxValue(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalEndValue: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret = temporal_max_value(temp);
            free(temp);
            return (int64_t)ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalValueN(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, int64_t, int64_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input, int64_t n, ValidityMask &mask, idx_t idx) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalValueN: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret;
            bool found = temporal_value_n(temp, n, &ret);
            if (!found) {
                free(temp);
                mask.SetInvalid(idx);
                return int64_t();
            }
            free(temp);
            return (int64_t)ret;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMinInstant(DataChunk &args, ExpressionState &state, Vector &result) {

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalMinInstant: unable to cast string to temporal");
                return string_t();
            }
            TInstant *ret = temporal_min_instant(temp);
            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, (Temporal*)ret, temp_size);
            free(temp);
            return string_t(temp_data, temp_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalMaxInstant(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalMaxInstant: unable to cast string to temporal");
                return string_t();
            }
            TInstant *ret = temporal_max_instant(temp);
            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, (Temporal*)ret, temp_size);
            free(temp);
            return string_t(temp_data, temp_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TInstantTimestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, timestamp_tz_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TInstantTimestamptz: unable to cast string to temporal");
                return timestamp_tz_t();
            }
            // ensure_temporal_isof_subtype(temp, TINSTANT);
            timestamp_tz_t ret = (timestamp_tz_t)((TInstant*)temp)->t;
            timestamp_tz_t duckdb_ts = TemporalHelpers::MeosToDuckDBTimestamp(ret);
            free(temp);
            return duckdb_ts;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalDuration(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, bool, interval_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input, bool boundspan) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalDuration: unable to cast string to temporal");
                return interval_t();
            }
            MeosInterval *ret = temporal_duration(temp, boundspan);
            interval_t duckdb_interval = TemporalHelpers::MeosToDuckDBInterval(ret);
            free(ret);
            free(temp);
            return duckdb_interval;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TsequenceConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &array_vec = args.data[0];
    array_vec.Flatten(count);
    auto *list_entries = ListVector::GetData(array_vec);
    auto &child_vec = ListVector::GetEntry(array_vec);

    meosType temptype = TemporalHelpers::GetTemptypeFromAlias(result.GetType().GetAlias().c_str());
    interpType interp = temptype_continuous(temptype) ? LINEAR : STEP;
    bool lower_inc = true;
    bool upper_inc = true;

    if (args.size() > 1) {
        auto &interp_child = args.data[1];
        interp_child.Flatten(count);
        auto interp_str = interp_child.GetValue(0).ToString();
        interp = interptype_from_string(interp_str.c_str());
    }
    if (args.size() > 2) {
        auto &lower_inc_child = args.data[2];
        lower_inc = lower_inc_child.GetValue(0).GetValue<bool>();
    }
    if (args.size() > 3) {
        auto &upper_inc_child = args.data[3];
        upper_inc = upper_inc_child.GetValue(0).GetValue<bool>();
    }

    child_vec.Flatten(ListVector::GetListSize(array_vec));
    auto child_data = FlatVector::GetData<string_t>(child_vec);

    UnaryExecutor::Execute<list_entry_t, string_t>(
        array_vec, result, count,
        [&](const list_entry_t &list) {
            auto offset = list.offset;
            auto length = list.length;
            TInstant **instants = (TInstant **)malloc(length * sizeof(TInstant *));
            if (!instants) {
                throw InternalException("Memory allocation failed in TsequenceConstructor");
            }
            for (idx_t i = 0; i < length; i++) {
                idx_t child_idx = offset + i;
                auto wkb_data = child_data[child_idx];
                Temporal *temp = nullptr;
                if (wkb_data.GetSize() > 0) {
                    temp = (Temporal*)malloc(wkb_data.GetSize());
                    memcpy(temp, wkb_data.GetDataUnsafe(), wkb_data.GetSize());
                }
                if (!temp) {
                    free(instants);
                    throw InternalException("Failure in TsequenceConstructor: unable to convert WKB to temporal");
                }
                instants[i] = (TInstant*)temp;
            }

            TSequence *seq = tsequence_make((const TInstant **)instants, length,
                lower_inc, upper_inc, interp, true);
            if (!seq) {
                for (idx_t j = 0; j < length; j++) {
                    free(instants[j]);
                }
                free(instants);
                throw InternalException("Failure in TsequenceConstructor: unable to create sequence");
            }

            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)seq) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)seq);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, (Temporal*)seq, temp_size);
            string_t result_str(temp_data, temp_size);
            free(seq);
            for (idx_t j = 0; j < length; j++) {
                free(instants[j]);
            }
            free(instants);
            return result_str;
        }
    );
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalToTsequence(DataChunk &args, ExpressionState &state, Vector &result) {
    interpType interp = INTERP_NONE;
    if (args.size() > 1) {
        auto &interp_child = args.data[1];
        interp_child.Flatten(args.size());
        auto interp_str = interp_child.GetValue(0).ToString();
        interp = interptype_from_string(interp_str.c_str());
    }

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalToTsequence: unable to cast string to temporal");
                return string_t();
            }
            TSequence *ret = temporal_to_tsequence(temp, interp);
            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, (Temporal*)ret, temp_size);
            string_t result_str(temp_data, temp_size);
            free(temp);
            return result_str;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TsequencesetConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &array_vec = args.data[0];
    array_vec.Flatten(count);
    auto *list_entries = ListVector::GetData(array_vec);
    auto &child_vec = ListVector::GetEntry(array_vec);

    child_vec.Flatten(ListVector::GetListSize(array_vec));
    auto child_data = FlatVector::GetData<string_t>(child_vec);

    UnaryExecutor::Execute<list_entry_t, string_t>(
        array_vec, result, count,
        [&](const list_entry_t &list) {
            auto offset = list.offset;
            auto length = list.length;

            TSequence **sequences = (TSequence **)malloc(length * sizeof(TSequence *));
            if (!sequences) {
                throw InternalException("Memory allocation failed in TsequencesetConstructor");
            }
            for (idx_t i = 0; i < length; i++) {
                idx_t child_idx = offset + i;
                auto wkb_data = child_data[child_idx];
                Temporal *temp = nullptr;
                if (wkb_data.GetSize() > 0) {
                    temp = (Temporal*)malloc(wkb_data.GetSize());
                    memcpy(temp, wkb_data.GetDataUnsafe(), wkb_data.GetSize());
                }
                if (!temp) {
                    free(sequences);
                    throw InternalException("Failure in TsequencesetConstructor: unable to convert WKB to temporal");
                }
                sequences[i] = (TSequence*)temp;
            }

            TSequenceSet *seqset = tsequenceset_make((const TSequence **)sequences, length, true);
            if (!seqset) {
                for (idx_t j = 0; j < length; j++) {
                    free(sequences[j]);
                }
                free(sequences);
                throw InternalException("Failure in TsequencesetConstructor: unable to create sequence set");
            }

            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)seqset) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)seqset);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, (Temporal*)seqset, temp_size);
            string_t result_str(temp_data, temp_size);
            free(seqset);
            for (idx_t j = 0; j < length; j++) {
                free(sequences[j]);
            }
            free(sequences);
            return result_str;
        }
    );
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalToTsequenceset(DataChunk &args, ExpressionState &state, Vector &result) {
    interpType interp = INTERP_NONE;
    if (args.size() > 1) {
        auto &interp_child = args.data[1];
        interp_child.Flatten(args.size());
        auto interp_str = interp_child.GetValue(0).ToString();
        interp = interptype_from_string(interp_str.c_str());
    }

    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalToTsequenceset: unable to cast string to temporal");
                return string_t();
            }
            TSequenceSet *ret = temporal_to_tsequenceset(temp, interp);
            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, (Temporal*)ret, temp_size);
            string_t result_str(temp_data, temp_size);
            free(temp);
            return result_str;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalToTstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalToTstzspan: unable to cast string to temporal");
                return string_t();
            }
            Span *ret = (Span*)malloc(sizeof(Span));
            temporal_set_tstzspan(temp, ret);
            size_t span_size = sizeof(*ret);
            uint8_t *span_buffer = (uint8_t*) malloc(span_size);
            memcpy(span_buffer, ret, span_size);
            string_t span_string_t((char *) span_buffer, span_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, span_string_t);
            free(span_buffer);
            free(ret);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberToSpan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TnumberToSpan: unable to cast string to temporal");
                return string_t();
            }
            Span *ret = tnumber_to_span(temp);
            size_t span_size = sizeof(*ret);
            uint8_t *span_buffer = (uint8_t*) malloc(span_size);
            memcpy(span_buffer, ret, span_size);
            string_t span_string_t((char *) span_buffer, span_size);
            string_t stored_data = StringVector::AddStringOrBlob(result, span_string_t);
            free(span_buffer);
            free(ret);
            return stored_data;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalValueset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in TemporalValueset: unable to cast string to temporal");
                return string_t();
            }
            int32_t count;
            Datum *values = temporal_values_p(temp, &count);
            meosType basetype = temptype_basetype((meosType)temp->temptype);
            if (temp->temptype == T_TBOOL) {
                // TODO: handle tbool
            }
            Set *ret = set_make_free(values, count, basetype, false);
            size_t total_size = set_mem_size(ret);
            string_t blob = StringVector::AddStringOrBlob(result, (const char*)ret, total_size);        
            free(ret);
            return blob;
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result) {
    idx_t total_count = 0;
    UnaryExecutor::Execute<string_t, list_entry_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in tint TemporalSequences: unable to cast string to temporal");
                return list_entry_t();
            }
            int32_t seq_count;
            const TSequence **sequences = temporal_sequences_p(temp, &seq_count);
            if (seq_count == 0) {
                free(temp);
                return list_entry_t();
            }
            const auto entry = list_entry_t(total_count, seq_count);
            total_count += seq_count;
            ListVector::Reserve(result, total_count);

            auto &seq_vec = ListVector::GetEntry(result);
            const auto seq_data = FlatVector::GetData<string_t>(seq_vec);

            for (idx_t i = 0; i < seq_count; i++) {
                const TSequence *seq = sequences[i];
                // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)seq) + VARHDRSZ;
                size_t temp_size = temporal_mem_size((Temporal*)seq);
                char *temp_data = (char*)malloc(temp_size);
                memcpy(temp_data, (Temporal*)seq, temp_size);
                seq_data[entry.offset + i] = string_t(temp_data, temp_size);
            }
            free(temp);
            return entry;
        }
    );
    ListVector::SetListSize(result, total_count);
}

void TemporalFunctions::TnumberShiftValue(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int64_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input, int64_t shift) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in tint TnumberShiftValue: unable to cast string to temporal");
                return string_t();
            }
            Temporal *ret = tnumber_shift_scale_value(temp, shift, 0, true, false);
            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, ret, temp_size);
            free(ret);
            free(temp);
            return string_t(temp_data, temp_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberScaleValue(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int64_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input, int64_t duration) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in tint TnumberScaleValue: unable to cast string to temporal");
                return string_t();
            }
            Temporal *ret = tnumber_shift_scale_value(temp, 0, duration, false, true);
            // size_t temp_size = VARSIZE_ANY_EXHDR((Temporal*)ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size((Temporal*)ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, ret, temp_size);
            free(ret);
            free(temp);
            return string_t(temp_data, temp_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TemporalFunctions::TnumberShiftScaleValue(DataChunk &args, ExpressionState &state, Vector &result) {
    TernaryExecutor::Execute<string_t, int64_t, int64_t, string_t>(
        args.data[0], args.data[1], args.data[2], result, args.size(),
        [&](string_t input, int64_t shift, int64_t duration) {
            Temporal *temp = nullptr;
            if (input.GetSize() > 0) {
                temp = (Temporal*)malloc(input.GetSize());
                memcpy(temp, input.GetDataUnsafe(), input.GetSize());
            }
            if (!temp) {
                throw InternalException("Failure in tint TnumberShiftScaleValue: unable to cast string to temporal");
                return string_t();
            }
            Temporal *ret = tnumber_shift_scale_value(temp, shift, duration, true, true);
            // size_t temp_size = VARSIZE_ANY_EXHDR(ret) + VARHDRSZ;
            size_t temp_size = temporal_mem_size(ret);
            char *temp_data = (char*)malloc(temp_size);
            memcpy(temp_data, ret, temp_size);
            free(ret);
            free(temp);
            return string_t(temp_data, temp_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

} // namespace duckdb