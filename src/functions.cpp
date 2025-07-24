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
}

namespace duckdb {

struct TIntFunctions {
    static void ExecuteTintMake(DataChunk &args, ExpressionState &state, Vector &result) {
        auto count = args.size();
        auto &value_vec = args.data[0];
        auto &t_vec = args.data[1];

        value_vec.Flatten(count);
        t_vec.Flatten(count);

        auto &children = StructVector::GetEntries(result);
        auto &value_child = children[0];
        auto &temptype_child = children[1];
        auto &t_child = children[2];

        for (idx_t i = 0; i < count; i++) {
            auto value = value_vec.GetValue(i).GetValue<int64_t>();
            auto t = t_vec.GetValue(i).GetValue<timestamp_tz_t>();
            TInstant *inst = tinstant_make((Datum)value, T_TINT, (TimestampTz)t.value);
            value_child->SetValue(i, Value::BIGINT((int64_t)inst->value));
            temptype_child->SetValue(i, Value::UTINYINT(inst->temptype));
            t_child->SetValue(i, Value::TIMESTAMPTZ(timestamp_tz_t(inst->t)));
            free(inst);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
    }

    static bool ExecuteTIntInFromString(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
        source.Flatten(count);

        auto &children = StructVector::GetEntries(result);
        auto &value_child = children[0];
        auto &temptype_child = children[1];
        auto &t_child = children[2];

        for (idx_t i = 0; i < count; i++) {
            auto input_str = source.GetValue(i).ToString();
            Temporal *ret = temporal_in(input_str.c_str(), T_TINT);
            TInstant *inst = (TInstant*)ret;
            value_child->SetValue(i, Value::BIGINT((int64_t)inst->value));
            temptype_child->SetValue(i, Value::UTINYINT(inst->temptype));
            t_child->SetValue(i, Value::TIMESTAMPTZ(timestamp_tz_t(inst->t)));
            free(inst);
        }
        if (count == 1) {
            result.SetVectorType(VectorType::CONSTANT_VECTOR);
        }
        // TODO: handle properly
        return true;
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
        auto &value_child = children[0];
        auto &temptype_child = children[1];
        auto &t_child = children[2];

        for (idx_t i = 0; i < count; i++) {
            auto value = value_child->GetValue(i).GetValue<int64_t>();
            auto temptype = temptype_child->GetValue(i).GetValue<uint8_t>();
            auto t = t_child->GetValue(i).GetValue<timestamp_tz_t>().value;
            TInstant *inst = tinstant_make((Datum)value, (meosType)temptype, (TimestampTz)t);
            Datum val = tinstant_value(inst);
            result.SetValue(i, Value::BIGINT((int64_t)val));
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
        auto &value_child = children[0];
        auto &temptype_child = children[1];
        auto &t_child = children[2];

        for (idx_t i = 0; i < count; i++) {
            auto value = value_child->GetValue(i).GetValue<int64_t>();
            auto temptype = temptype_child->GetValue(i).GetValue<uint8_t>();
            auto t = t_child->GetValue(i).GetValue<timestamp_tz_t>().value;
            TInstant *inst = tinstant_make((Datum)value, (meosType)temptype, (TimestampTz)t);
            int maxdd = 15;
            char *str = temporal_out((Temporal*)inst, maxdd);
            result.SetValue(i, Value(str));
            free(str);
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
}

} // namespace duckdb