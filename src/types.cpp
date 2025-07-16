#define MOBILITYDUCK_EXTENSION_TYPES

#include "common.hpp"
#include "types.hpp"
#include "duckdb/common/extension_type_info.hpp"

extern "C" {
    #include "meos/meos.h"
}

namespace duckdb {

LogicalType GeoTypes::TINSTANT() {
    auto type = LogicalType::STRUCT({
        {"value", LogicalType::BIGINT},  // Using BIGINT for Datum for now
        {"temptype", LogicalType::UTINYINT},
        {"t", LogicalType::TIMESTAMP_TZ}});
    type.SetAlias("TINSTANT");
    return type;
}

// SQL function implementations
inline void ExecuteTInstantMake(DataChunk &args, ExpressionState &state, Vector &result) {
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

        TInstant *inst = tinstant_make((Datum)value, temptype, (TimestampTz)t.value);
        value_child->SetValue(i, Value::BIGINT((int64_t)inst->value));
        temptype_child->SetValue(i, Value::UTINYINT(inst->temptype));
        t_child->SetValue(i, Value::TIMESTAMPTZ(timestamp_tz_t(inst->t)));
        free(inst);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTInstantValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tinstant_vec = args.data[0];
    tinstant_vec.Flatten(count);

    auto &children = StructVector::GetEntries(tinstant_vec);
    auto &value_child = children[0];
    auto &temptype_child = children[1];
    auto &t_child = children[2];

    for (idx_t i = 0; i < count; i++) {
        TInstant inst;
        inst.value = value_child->GetValue(i).GetValue<int64_t>();
        inst.temptype = temptype_child->GetValue(i).GetValue<uint8_t>();
        inst.t = t_child->GetValue(i).GetValue<timestamp_tz_t>().value;
        Datum val = tinstant_value(&inst);
        result.SetValue(i, Value::BIGINT((int64_t)val));
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTInstantToString(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &tinstant_vec = args.data[0];
    tinstant_vec.Flatten(count);

    auto &children = StructVector::GetEntries(tinstant_vec);
    auto &value_child = children[0];
    auto &temptype_child = children[1];
    auto &t_child = children[2];

    for (idx_t i = 0; i < count; i++) {
        TInstant inst;
        inst.value = value_child->GetValue(i).GetValue<int64_t>();
        inst.temptype = temptype_child->GetValue(i).GetValue<uint8_t>();
        inst.t = t_child->GetValue(i).GetValue<timestamp_tz_t>().value;
        char *str = tinstant_to_string(&inst, 0, value_out_int);
        result.SetValue(i, Value(str));
        free(str);
    }
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void GeoTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    // Register TInstant functions
    auto tinstant_make_function = ScalarFunction(
        "TINSTANT_MAKE", // name
        {LogicalType::BIGINT, LogicalType::UTINYINT, LogicalType::TIMESTAMP_TZ}, // arguments
        GeoTypes::TINSTANT(), // return type
        ExecuteTInstantMake); // function
    ExtensionUtil::RegisterFunction(instance, tinstant_make_function);

    auto tinstant_value_function = ScalarFunction(
        "TINSTANT_VALUE", // name
        {GeoTypes::TINSTANT()}, // arguments
        LogicalType::BIGINT, // return type
        ExecuteTInstantValue); // function
    ExtensionUtil::RegisterFunction(instance, tinstant_value_function);

    auto tinstant_to_string_function = ScalarFunction(
        "TINSTANT_TO_STRING", // name
        {GeoTypes::TINSTANT()}, // arguments
        LogicalType::VARCHAR, // return type
        ExecuteTInstantToString); // function
    ExtensionUtil::RegisterFunction(instance, tinstant_to_string_function);
}

void GeoTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINSTANT", GeoTypes::TINSTANT());
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif