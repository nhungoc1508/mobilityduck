#include "common.hpp"
#include "temporal/tint.hpp"
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

void TInt::Serialize(Serializer &serializer) const {
    if (IsNull()) {
        serializer.WriteProperty<bool>(1, "is_null", true);
        return;
    }
    serializer.WriteProperty<bool>(1, "is_null", false);
    Temporal *temp = GetTemporal();
    char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
    serializer.WriteProperty<string>(2, "meos_ptr", str);
    free(str);
}

void TInt::Deserialize(Deserializer &deserializer) {
    bool is_null = deserializer.ReadProperty<bool>(1, "is_null");
    if (is_null) {
        meos_ptr = 0;
        return;
    }
    string str = deserializer.ReadProperty<string>(2, "meos_ptr");
    Temporal *temp = temporal_in(str.c_str(), T_TINT);
    meos_ptr = (uintptr_t)temp;
}

Temporal* TInt::GetTemporal() const {
    return (Temporal*)meos_ptr;
}

tempSubtype TInt::GetSubtype() const {
    if (IsNull()) return ANYTEMPSUBTYPE;
    return (tempSubtype)GetTemporal()->subtype;
}

meosType TInt::GetTemptype() const {
    if (IsNull()) return T_TINT;
    return (meosType)GetTemporal()->temptype;
}

bool TInt::IsNull() const {
    return meos_ptr == 0;
}

TInt TInt::FromTemporal(Temporal *temp) {
    return TInt((uintptr_t)temp);
}

TInt TInt::FromString(const string &str) {
    Temporal *temp = temporal_in(str.c_str(), T_TINT);
    return TInt((uintptr_t)temp);
}

string TInt::ToString(const TInt &tint) {
    if (tint.IsNull()) return "NULL";
    
    Temporal *temp = tint.GetTemporal();
    char *str = temporal_out(temp, OUT_DEFAULT_DECIMAL_DIGITS);
    string result(str);
    free(str);
    return result;
}

size_t TIntHash::operator()(const TInt &tint) const {
    if (tint.IsNull()) return 0;
    Temporal *temp = tint.GetTemporal();
    return std::hash<string>{}(TInt::ToString(tint));
}

LogicalType TInt::TIntMake() {
    auto type = LogicalType::STRUCT({
        {"meos_ptr", LogicalType::UBIGINT}
    });
    type.SetAlias("TINT");
    return type;
}

void TInt::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINT", TInt::TIntMake());
}

void TInt::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TInt::TIntMake(),
        TemporalFunctions::StringToTemporal
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TInt::TIntMake(),
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalToString
    );
}

void TInt::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto constructor = ScalarFunction(
        "tint",
        {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ},
        TInt::TIntMake(),
        TemporalFunctions::TInstantConstructor
    );
    ExtensionUtil::RegisterFunction(instance, constructor);

    auto tempsubtype = ScalarFunction(
        "tempSubtype",
        {TInt::TIntMake()},
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalSubtype
    );
    ExtensionUtil::RegisterFunction(instance, tempsubtype);

    auto interp = ScalarFunction(
        "interp",
        {TInt::TIntMake()},
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalInterp
    );
    ExtensionUtil::RegisterFunction(instance, interp);

    auto value = ScalarFunction(
        "getValue",
        {TInt::TIntMake()},
        LogicalType::BIGINT,
        TemporalFunctions::TInstantValue
    );
    ExtensionUtil::RegisterFunction(instance, value);

    auto start_value = ScalarFunction(
        "startValue",
        {TInt::TIntMake()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalStartValue
    );
    ExtensionUtil::RegisterFunction(instance, start_value);

    auto end_value = ScalarFunction(
        "endValue",
        {TInt::TIntMake()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalEndValue
    );
    ExtensionUtil::RegisterFunction(instance, end_value);
}

} // namespace duckdb