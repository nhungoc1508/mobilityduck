#include "meos_wrapper_simple.hpp"

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
    // #include <postgres.h>
    // #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_internal.h>
    // #include "temporal/type_inout.h"
}

namespace duckdb {

LogicalType TInt::TINT() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("TINT");
    return type;
}

void TInt::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINT", TInt::TINT());
}

void TInt::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TInt::TINT(),
        TemporalFunctions::StringToTemporal
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TInt::TINT(),
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalToString
    );
}

void TInt::RegisterScalarFunctions(DatabaseInstance &instance) {
    auto constructor = ScalarFunction(
        "tint",
        {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ},
        TInt::TINT(),
        TemporalFunctions::TInstantConstructor
    );
    ExtensionUtil::RegisterFunction(instance, constructor);

    auto tempsubtype = ScalarFunction(
        "tempSubtype",
        {TInt::TINT()},
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalSubtype
    );
    ExtensionUtil::RegisterFunction(instance, tempsubtype);

    auto interp = ScalarFunction(
        "interp",
        {TInt::TINT()},
        LogicalType::VARCHAR,
        TemporalFunctions::TemporalInterp
    );
    ExtensionUtil::RegisterFunction(instance, interp);

    auto value = ScalarFunction(
        "getValue",
        {TInt::TINT()},
        LogicalType::BIGINT,
        TemporalFunctions::TInstantValue
    );
    ExtensionUtil::RegisterFunction(instance, value);

    auto start_value = ScalarFunction(
        "startValue",
        {TInt::TINT()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalStartValue
    );
    ExtensionUtil::RegisterFunction(instance, start_value);

    auto end_value = ScalarFunction(
        "endValue",
        {TInt::TINT()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalEndValue
    );
    ExtensionUtil::RegisterFunction(instance, end_value);

    auto min_value = ScalarFunction(
        "minValue",
        {TInt::TINT()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalMinValue
    );
    ExtensionUtil::RegisterFunction(instance, min_value);

    auto max_value = ScalarFunction(
        "maxValue",
        {TInt::TINT()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalMaxValue
    );
    ExtensionUtil::RegisterFunction(instance, max_value);

    auto value_N = ScalarFunction(
        "valueN",
        {TInt::TINT(), LogicalType::BIGINT},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalValueN
    );
    ExtensionUtil::RegisterFunction(instance, value_N);

    auto min_instant = ScalarFunction(
        "minInstant",
        {TInt::TINT()},
        TInt::TINT(),
        TemporalFunctions::TemporalMinInstant
    );
    ExtensionUtil::RegisterFunction(instance, min_instant);

    auto max_instant = ScalarFunction(
        "maxInstant",
        {TInt::TINT()},
        TInt::TINT(),
        TemporalFunctions::TemporalMaxInstant
    );
    ExtensionUtil::RegisterFunction(instance, max_instant);

    auto instant_timestamptz = ScalarFunction(
        "getTimestamp",
        {TInt::TINT()},
        LogicalType::TIMESTAMP_TZ,
        TemporalFunctions::TInstantTimestamptz
    );
    ExtensionUtil::RegisterFunction(instance, instant_timestamptz);

    auto duration = ScalarFunction(
        "duration",
        {TInt::TINT(), LogicalType::BOOLEAN},
        LogicalType::INTERVAL,
        TemporalFunctions::TemporalDuration
    );
    ExtensionUtil::RegisterFunction(instance, duration);

    auto tseq_constructor = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TINT())},
        TInt::TINT(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor);

    auto tseq_constructor2 = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TINT()), LogicalType::VARCHAR},
        TInt::TINT(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor2);

    auto tseq_constructor3 = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TINT()), LogicalType::VARCHAR, LogicalType::BOOLEAN},
        TInt::TINT(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor3);

    auto tseq_constructor4 = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TINT()), LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
        TInt::TINT(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor4);

    auto temp_to_tseq = ScalarFunction(
        "tintSeq",
        {TInt::TINT(), LogicalType::VARCHAR},
        TInt::TINT(),
        TemporalFunctions::TemporalToTsequence
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tseq);

    auto temp_to_tseq2 = ScalarFunction(
        "tintSeq",
        {TInt::TINT()},
        TInt::TINT(),
        TemporalFunctions::TemporalToTsequence
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tseq2);

    auto tseqset_constructor = ScalarFunction(
        "tintSeqSet",
        {LogicalType::LIST(TInt::TINT())},
        TInt::TINT(),
        TemporalFunctions::TsequencesetConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseqset_constructor);

    auto temp_to_tseqset = ScalarFunction(
        "tintSeqSet",
        {TInt::TINT()},
        TInt::TINT(),
        TemporalFunctions::TemporalToTsequenceset
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tseqset);

    auto temp_to_tstzspan = ScalarFunction(
        "timeSpan",
        {TInt::TINT()},
        SpanType::SPAN(),
        TemporalFunctions::TemporalToTstzspan
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tstzspan);

    auto tnumber_to_span = ScalarFunction(
        "valueSpan",
        {TInt::TINT()},
        SpanType::SPAN(),
        TemporalFunctions::TnumberToSpan
    );
    ExtensionUtil::RegisterFunction(instance, tnumber_to_span);

    auto valueset = ScalarFunction(
        "valueSet",
        {TInt::TINT()},
        SetTypes::INTSET(),
        TemporalFunctions::TemporalValueset
    );
    ExtensionUtil::RegisterFunction(instance, valueset);

    auto sequences = ScalarFunction(
        "sequences",
        {TInt::TINT()},
        LogicalType::LIST(TInt::TINT()),
        TemporalFunctions::TemporalSequences
    );
    ExtensionUtil::RegisterFunction(instance, sequences);

    auto shift_value = ScalarFunction(
        "shiftValue",
        {TInt::TINT(), LogicalType::BIGINT},
        TInt::TINT(),
        TemporalFunctions::TnumberShiftValue
    );
    ExtensionUtil::RegisterFunction(instance, shift_value);

    auto scale_value = ScalarFunction(
        "scaleValue",
        {TInt::TINT(), LogicalType::BIGINT},
        TInt::TINT(),
        TemporalFunctions::TnumberScaleValue
    );
    ExtensionUtil::RegisterFunction(instance, scale_value);

    auto shift_scale_value = ScalarFunction(
        "shiftScaleValue",
        {TInt::TINT(), LogicalType::BIGINT, LogicalType::BIGINT},
        TInt::TINT(),
        TemporalFunctions::TnumberShiftScaleValue
    );
    ExtensionUtil::RegisterFunction(instance, shift_scale_value);
}

} // namespace duckdb