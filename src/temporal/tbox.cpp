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
    ExtensionLoader::RegisterType(instance, "TBOX", TBOX());
}

void TboxType::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionLoader::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TBOX(),
        TboxFunctions::Tbox_in
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        TBOX(),
        LogicalType::VARCHAR,
        TboxFunctions::Tbox_out
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        LogicalType::INTEGER,
        TBOX(),
        TboxFunctions::Number_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        LogicalType::DOUBLE,
        TBOX(),
        TboxFunctions::Number_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        LogicalType::TIMESTAMP_TZ,
        TBOX(),
        TboxFunctions::Timestamptz_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        SetTypes::intset(),
        TBOX(),
        TboxFunctions::Set_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        SetTypes::floatset(),
        TBOX(),
        TboxFunctions::Set_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        SetTypes::tstzset(),
        TBOX(),
        TboxFunctions::Set_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        SpanTypes::INTSPAN(),
        TBOX(),
        TboxFunctions::Span_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        SpanTypes::FLOATSPAN(),
        TBOX(),
        TboxFunctions::Span_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        SpanTypes::TSTZSPAN(),
        TBOX(),
        TboxFunctions::Span_to_tbox_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        TBOX(),
        SpanTypes::INTSPAN(),
        TboxFunctions::Tbox_to_intspan_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        TBOX(),
        SpanTypes::FLOATSPAN(),
        TboxFunctions::Tbox_to_floatspan_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        TBOX(),
        SpanTypes::TSTZSPAN(),
        TboxFunctions::Tbox_to_tstzspan_cast
    );
}

void TboxType::RegisterScalarFunctions(DatabaseInstance &instance) {
    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::INTEGER, LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Number_timestamptz_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Number_timestamptz_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::INTSPAN(), LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Numspan_timestamptz_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::FLOATSPAN(), LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Numspan_timestamptz_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::INTEGER, SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Number_tstzspan_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::DOUBLE, SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Number_tstzspan_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::INTSPAN(), SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Numspan_tstzspan_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::FLOATSPAN(), SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Numspan_tstzspan_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::INTEGER},
            TBOX(),
            TboxFunctions::Number_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::DOUBLE},
            TBOX(),
            TboxFunctions::Number_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {LogicalType::TIMESTAMP_TZ},
            TBOX(),
            TboxFunctions::Timestamptz_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SetTypes::intset()},
            TBOX(),
            TboxFunctions::Set_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SetTypes::floatset()},
            TBOX(),
            TboxFunctions::Set_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SetTypes::tstzset()},
            TBOX(),
            TboxFunctions::Set_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::INTSPAN()},
            TBOX(),
            TboxFunctions::Span_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::FLOATSPAN()},
            TBOX(),
            TboxFunctions::Span_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tbox",
            {SpanTypes::TSTZSPAN()},
            TBOX(),
            TboxFunctions::Span_to_tbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "intspan",
            {TBOX()},
            SpanTypes::INTSPAN(),
            TboxFunctions::Tbox_to_intspan
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "floatspan",
            {TBOX()},
            SpanTypes::FLOATSPAN(),
            TboxFunctions::Tbox_to_floatspan
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "timeSpan",
            {TBOX()},
            SpanTypes::TSTZSPAN(),
            TboxFunctions::Tbox_to_tstzspan
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "hasX",
            {TBOX()},
            LogicalType::BOOLEAN,
            TboxFunctions::Tbox_hasx
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "hasT",
            {TBOX()},
            LogicalType::BOOLEAN,
            TboxFunctions::Tbox_hast
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "Xmin",
            {TBOX()},
            LogicalType::DOUBLE,
            TboxFunctions::Tbox_xmin
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "XminInc",
            {TBOX()},
            LogicalType::BOOLEAN,
            TboxFunctions::Tbox_xmin_inc
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "Xmax",
            {TBOX()},
            LogicalType::DOUBLE,
            TboxFunctions::Tbox_xmax
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "XmaxInc",
            {TBOX()},
            LogicalType::BOOLEAN,
            TboxFunctions::Tbox_xmax_inc
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "Tmin",
            {TBOX()},
            LogicalType::TIMESTAMP_TZ,
            TboxFunctions::Tbox_tmin
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "TminInc",
            {TBOX()},
            LogicalType::BOOLEAN,
            TboxFunctions::Tbox_tmin_inc
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "Tmax",
            {TBOX()},
            LogicalType::TIMESTAMP_TZ,
            TboxFunctions::Tbox_tmax
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "TmaxInc",
            {TBOX()},
            LogicalType::BOOLEAN,
            TboxFunctions::Tbox_tmax_inc
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "shiftValue",
            {TBOX(), LogicalType::INTEGER},
            TBOX(),
            TboxFunctions::Tbox_shift_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "shiftValue",
            {TBOX(), LogicalType::DOUBLE},
            TBOX(),
            TboxFunctions::Tbox_shift_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "shiftTime",
            {TBOX(), LogicalType::INTERVAL},
            TBOX(),
            TboxFunctions::Tbox_shift_time
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "scaleValue",
            {TBOX(), LogicalType::INTEGER},
            TBOX(),
            TboxFunctions::Tbox_scale_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "scaleValue",
            {TBOX(), LogicalType::DOUBLE},
            TBOX(),
            TboxFunctions::Tbox_scale_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "scaleTime",
            {TBOX(), LogicalType::INTERVAL},
            TBOX(),
            TboxFunctions::Tbox_scale_time
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "shiftScaleValue",
            {TBOX(), LogicalType::INTEGER, LogicalType::INTEGER},
            TBOX(),
            TboxFunctions::Tbox_shift_scale_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "shiftScaleValue",
            {TBOX(), LogicalType::DOUBLE, LogicalType::DOUBLE},
            TBOX(),
            TboxFunctions::Tbox_shift_scale_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "shiftScaleTime",
            {TBOX(), LogicalType::INTERVAL, LogicalType::INTERVAL},
            TBOX(),
            TboxFunctions::Tbox_shift_scale_time
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "expandValue",
            {TBOX(), LogicalType::INTEGER},
            TBOX(),
            TboxFunctions::Tbox_expand_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "expandValue",
            {TBOX(), LogicalType::DOUBLE},
            TBOX(),
            TboxFunctions::Tbox_expand_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "expandTime",
            {TBOX(), LogicalType::INTERVAL},
            TBOX(),
            TboxFunctions::Tbox_expand_time
        )
    );
}

} // namespace duckdb