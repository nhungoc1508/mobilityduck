#include "common.hpp"
#include "temporal/tbool.hpp"
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

LogicalType TBool::TBoolMake() {
    auto type = LogicalType::STRUCT({
        {"meos_ptr", LogicalType::UBIGINT}
    });
    type.SetAlias("TBOOL");
    return type;
}

void TBool::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TBOOL", TBool::TBoolMake());
}

void TBool::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TBool::TBoolMake(),
        TemporalFunctions::StringToTemporal
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TBool::TBoolMake(),
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalToString
    );
}

void TBool::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto constructor = ScalarFunction(
        "tbool",
        {LogicalType::BOOLEAN, LogicalType::TIMESTAMP_TZ},
        TBool::TBoolMake(),
        TemporalFunctions::TInstantConstructor
    );
    ExtensionUtil::RegisterFunction(instance, constructor);

    auto tempsubtype = ScalarFunction(
        "tempSubtype",
        {TBool::TBoolMake()},
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalSubtype
    );
    ExtensionUtil::RegisterFunction(instance, tempsubtype);

    auto interp = ScalarFunction(
        "interp",
        {TBool::TBoolMake()},
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalInterp
    );
    ExtensionUtil::RegisterFunction(instance, interp);

    auto value = ScalarFunction(
        "getValue",
        {TBool::TBoolMake()},
        LogicalType::BOOLEAN,
        TemporalFunctions::TInstantValue
    );
    ExtensionUtil::RegisterFunction(instance, value);

    auto start_value = ScalarFunction(
        "startValue",
        {TBool::TBoolMake()},
        LogicalType::BOOLEAN,
        TemporalFunctions::TemporalStartValue
    );
    ExtensionUtil::RegisterFunction(instance, start_value);

    auto end_value = ScalarFunction(
        "endValue",
        {TBool::TBoolMake()},
        LogicalType::BOOLEAN,
        TemporalFunctions::TemporalEndValue
    );
    ExtensionUtil::RegisterFunction(instance, end_value);
}
} // namespace duckdb