#include "common.hpp"
#include "temporal/tint3.hpp"
#include "temporal/temporal_functions.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/cast/default_casts.hpp"

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_internal.h>
    #include "temporal/type_inout.h"
}

namespace duckdb {

LogicalType TInt3::TInt3Make() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("TINT3");
    return type;
}

void TInt3::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINT3", TInt3::TInt3Make());
}

bool TInt3::StringToTemporal(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    printf("StringToTemporal called\n");
    bool success = true;
    try {
        UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
            source, result, count, [&](string_t input, ValidityMask &mask, idx_t idx) {
                if (input.GetSize() == 0) {
                    return string_t();
                }
                std::string input_str(input.GetDataUnsafe(), input.GetSize());
                Temporal *temp = temporal_in(input_str.c_str(), T_TINT);
                if (!temp) {
                    throw InternalException("Failure in tint cast: unable to cast string to temporal");
                    success = false;
                    return string_t();
                }
                size_t wkb_size;
                uint8_t *wkb = temporal_as_wkb(temp, WKB_EXTENDED, &wkb_size);
                if (!wkb) {
                    throw InternalException("Failure in tint cast: unable to cast temporal to wkb");
                    success = false;
                    return string_t();
                }
                return string_t((const char*)wkb, wkb_size);
        });
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        return false;
    }
    return success;
}

bool TInt3::TemporalToString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    printf("TemporalToString called\n");
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count, [&](string_t input) {
            uint8_t *wkb = (uint8_t *)input.GetDataUnsafe();
            size_t wkb_size = input.GetSize();
            Temporal *temp = temporal_from_wkb(wkb, wkb_size);
            if (!temp) {
                throw InternalException("Failure in tint cast: unable to cast string to temporal");
                return string_t();
            }
            char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
            free(temp);
            return string_t(str);
        });
    return true;
}

void TInt3::RegisterCastFunctions(DatabaseInstance &instance) {
    // VARCHAR -> TINT
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TInt3::TInt3Make(),
        TInt3::StringToTemporal
    );

    // TINT -> VARCHAR
    ExtensionUtil::RegisterCastFunction(
        instance,
        TInt3::TInt3Make(),
        LogicalType::VARCHAR,
        TInt3::TemporalToString
    );
}

void TInt3::TInstantConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    // printf("TInstantConstructor called\n");
    BinaryExecutor::Execute<int64_t, timestamp_tz_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](int64_t value, timestamp_tz_t ts) {
            meosType temptype = T_TINT;
            TInstant *inst = tinstant_make((Datum)value, temptype, (TimestampTz)ts.value);
            size_t wkb_size;
            uint8_t *wkb = temporal_as_wkb((Temporal*)inst, WKB_EXTENDED, &wkb_size);
            if (!wkb) {
                throw InternalException("Failure in tint constructor: unable to cast temporal to wkb");
                return string_t();
            }
            return string_t((const char*)wkb, wkb_size);
        });
}

void TInt3::TemporalSubtype(DataChunk &args, ExpressionState &state, Vector &result) {
    // printf("TemporalSubtype called\n");
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            uint8_t *wkb = (uint8_t *)input.GetDataUnsafe();
            size_t wkb_size = input.GetSize();
            Temporal *temp = temporal_from_wkb(wkb, wkb_size);
            if (!temp) {
                throw InternalException("Failure in tint TemporalSubtype: unable to cast string to temporal");
                return string_t();
            }
            tempSubtype subtype = (tempSubtype)temp->subtype;
            const char *str = tempsubtype_name(subtype);
            free(temp);
            return string_t(str);
        });
}

void TInt3::TInstantValue(DataChunk &args, ExpressionState &state, Vector &result) {
    // printf("TInstantValue called\n");
    UnaryExecutor::Execute<string_t, int64_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            uint8_t *wkb = (uint8_t *)input.GetDataUnsafe();
            size_t wkb_size = input.GetSize();
            Temporal *temp = temporal_from_wkb(wkb, wkb_size);
            if (!temp) {
                throw InternalException("Failure in tint TInstantValue: unable to cast string to temporal");
                return int64_t();
            }
            Datum ret = tinstant_value((TInstant*)temp);
            free(temp);
            return (int64_t)ret;
        }
    );
}

void TInt3::TInstantTimestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    // printf("TInstantTimestamptz called\n");
    UnaryExecutor::Execute<string_t, timestamp_tz_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            uint8_t *wkb = (uint8_t *)input.GetDataUnsafe();
            size_t wkb_size = input.GetSize();
            Temporal *temp = temporal_from_wkb(wkb, wkb_size);
            if (!temp) {
                throw InternalException("Failure in tint TInstantTimestamptz: unable to cast string to temporal");
                return timestamp_tz_t();
            }
            ensure_temporal_isof_subtype(temp, TINSTANT);
            timestamp_tz_t ret = (timestamp_tz_t)((TInstant*)temp)->t;
            free(temp);
            return ret;
        }
    );
}

vector<Value> TInt3::TempArrToArray(Temporal **temparr, int32_t count, LogicalType element_type) {
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

// void TInt3::TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result) {
//     printf("TemporalSequences called\n");
//     UnaryExecutor::Execute<string_t, list_entry_t>(
//         args.data[0], result, args.size(),
//         [&](string_t input) {
//             auto wkb_data = (uint8_t *)input.GetDataUnsafe();
//             auto wkb_size = input.GetSize();
//             Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
//             if (!temp) {
//                 throw InternalException("Failure in tint TemporalSequences: unable to cast string to temporal");
//                 return list_entry_t();
//             }
//             int32_t seq_count;
//             const TSequence **sequences = temporal_sequences_p(temp, &seq_count);
//             printf("Here 0, seq_count: %d\n", seq_count);
//             if (seq_count == 0) {
//                 free(temp);
//                 return list_entry_t();
//             }

//             auto &child_vec = ListVector::GetEntry(result);
//             ListVector::Reserve(result, seq_count);
//             auto child_data = FlatVector::GetData<string_t>(child_vec);
//             printf("Here 1\n");

//             for (int32_t i = 0; i < seq_count; i++) {
//                 const TSequence *seq = sequences[i];
//                 size_t seq_wkb_size;
//                 uint8_t *seq_wkb = temporal_as_wkb((Temporal*)seq, WKB_EXTENDED, &seq_wkb_size);
//                 auto seq_string = string_t((const char*)seq_wkb, seq_wkb_size);
//                 printf("Here 2\n");

//                 child_data[i] = seq_string;

//                 free(seq_wkb);
//             }

//             free(temp);
//             return list_entry_t(0, seq_count);
//         }
//     );
// }

void TInt3::TemporalSequences(DataChunk &args, ExpressionState &state, Vector &result) {
    printf("TemporalSequences called\n");
    idx_t total_count = 0;
    UnaryExecutor::Execute<string_t, list_entry_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            auto wkb_data = (uint8_t *)input.GetDataUnsafe();
            auto wkb_size = input.GetSize();
            Temporal *temp = temporal_from_wkb(wkb_data, wkb_size);
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
                size_t seq_wkb_size;
                uint8_t *seq_wkb = temporal_as_wkb((Temporal*)seq, WKB_EXTENDED, &seq_wkb_size);
                seq_data[entry.offset + i] = string_t((const char*)seq_wkb, seq_wkb_size);
            }
            free(temp);
            return entry;
        }
    );
    ListVector::SetListSize(result, total_count);
}

void TInt3::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto constructor = ScalarFunction(
        "tint3",
        {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ},
        TInt3::TInt3Make(),
        TInt3::TInstantConstructor
    );
    ExtensionUtil::RegisterFunction(instance, constructor);

    auto temp_subtype = ScalarFunction(
        "tempSubtype",
        {TInt3::TInt3Make()},
        LogicalType::VARCHAR,
        TInt3::TemporalSubtype
    );
    ExtensionUtil::RegisterFunction(instance, temp_subtype);

    auto get_value = ScalarFunction(
        "getValue",
        {TInt3::TInt3Make()},
        LogicalType::BIGINT,
        TInt3::TInstantValue
    );
    ExtensionUtil::RegisterFunction(instance, get_value);

    auto instant_timestamptz = ScalarFunction(
        "getTimestamp",
        {TInt3::TInt3Make()},
        LogicalType::TIMESTAMP_TZ,
        TInt3::TInstantTimestamptz
    );
    ExtensionUtil::RegisterFunction(instance, instant_timestamptz);

    auto sequences = ScalarFunction(
        "sequences",
        {TInt3::TInt3Make()},
        LogicalType::LIST(TInt3::TInt3Make()),
        TInt3::TemporalSequences
    );
    ExtensionUtil::RegisterFunction(instance, sequences);
}

} // namespace duckdb