#include "meos_wrapper_simple.hpp"

#include "common.hpp"
#include "temporal/tbox.hpp"
#include "temporal/tbox_functions.hpp"

#include "duckdb/common/types/blob.hpp"
// #include "duckdb/common/exception.hpp"
// #include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
// #include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

LogicalType TboxType::TBOX() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("TBOX");
    return type;
}

void TboxType::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TBOX", TBOX());
}

void TboxType::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TBOX(),
        TboxFunctions::Tbox_in
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TBOX(),
        LogicalType::VARCHAR,
        TboxFunctions::Tbox_out
    );
}

void TboxType::RegisterScalarFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::INTEGER, LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Number_timestamptz_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Number_timestamptz_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::INTSPAN(), LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Numspan_timestamptz_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::FLOATSPAN(), LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Numspan_timestamptz_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::INTEGER, SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Number_tstzspan_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::DOUBLE, SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Number_tstzspan_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::INTSPAN(), SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Numspan_tstzspan_to_tbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::FLOATSPAN(), SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Numspan_tstzspan_to_tbox
        )
    );
}

} // namespace duckdb