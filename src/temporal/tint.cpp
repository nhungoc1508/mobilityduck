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

LogicalType TInt::TIntMake() {
    auto type = LogicalType::STRUCT({
        {"meos_ptr", LogicalType::UBIGINT}
    });
    type.SetAlias("TINT");
    return type;
}

// LogicalType TInt::TIntMake() {
//     auto type = LogicalType(LogicalTypeId::VARCHAR);
//     type.SetAlias("TINT");
//     return type;
// }

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

    auto min_value = ScalarFunction(
        "minValue",
        {TInt::TIntMake()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalMinValue
    );
    ExtensionUtil::RegisterFunction(instance, min_value);

    auto max_value = ScalarFunction(
        "maxValue",
        {TInt::TIntMake()},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalMaxValue
    );
    ExtensionUtil::RegisterFunction(instance, max_value);

    auto value_N = ScalarFunction(
        "valueN",
        {TInt::TIntMake(), LogicalType::BIGINT},
        LogicalType::BIGINT,
        TemporalFunctions::TemporalValueN
    );
    ExtensionUtil::RegisterFunction(instance, value_N);

    auto min_instant = ScalarFunction(
        "minInstant",
        {TInt::TIntMake()},
        TInt::TIntMake(),
        TemporalFunctions::TemporalMinInstant
    );
    ExtensionUtil::RegisterFunction(instance, min_instant);

    auto max_instant = ScalarFunction(
        "maxInstant",
        {TInt::TIntMake()},
        TInt::TIntMake(),
        TemporalFunctions::TemporalMaxInstant
    );
    ExtensionUtil::RegisterFunction(instance, max_instant);

    auto instant_timestamptz = ScalarFunction(
        "getTimestamp",
        {TInt::TIntMake()},
        LogicalType::TIMESTAMP_TZ,
        TemporalFunctions::TInstantTimestamptz
    );
    ExtensionUtil::RegisterFunction(instance, instant_timestamptz);

    auto duration = ScalarFunction(
        "duration",
        {TInt::TIntMake(), LogicalType::BOOLEAN},
        LogicalType::INTERVAL,
        TemporalFunctions::TemporalDuration
    );
    ExtensionUtil::RegisterFunction(instance, duration);

    auto tseq_constructor = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TIntMake())},
        TInt::TIntMake(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor);

    auto tseq_constructor2 = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TIntMake()), LogicalType::VARCHAR},
        TInt::TIntMake(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor2);

    auto tseq_constructor3 = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TIntMake()), LogicalType::VARCHAR, LogicalType::BOOLEAN},
        TInt::TIntMake(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor3);

    auto tseq_constructor4 = ScalarFunction(
        "tintSeq",
        {LogicalType::LIST(TInt::TIntMake()), LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
        TInt::TIntMake(),
        TemporalFunctions::TsequenceConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseq_constructor4);

    auto temp_to_tseq = ScalarFunction(
        "tintSeq",
        {TInt::TIntMake(), LogicalType::VARCHAR},
        TInt::TIntMake(),
        TemporalFunctions::TemporalToTsequence
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tseq);

    auto temp_to_tseq2 = ScalarFunction(
        "tintSeq",
        {TInt::TIntMake()},
        TInt::TIntMake(),
        TemporalFunctions::TemporalToTsequence
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tseq2);

    auto tseqset_constructor = ScalarFunction(
        "tintSeqSet",
        {LogicalType::LIST(TInt::TIntMake())},
        TInt::TIntMake(),
        TemporalFunctions::TsequencesetConstructor
    );
    ExtensionUtil::RegisterFunction(instance, tseqset_constructor);

    auto temp_to_tseqset = ScalarFunction(
        "tintSeqSet",
        {TInt::TIntMake()},
        TInt::TIntMake(),
        TemporalFunctions::TemporalToTsequenceset
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tseqset);

    auto temp_to_tstzspan = ScalarFunction(
        "timeSpan",
        {TInt::TIntMake()},
        SpanType::SPAN(),
        TemporalFunctions::TemporalToTstzspan
    );
    ExtensionUtil::RegisterFunction(instance, temp_to_tstzspan);

    auto tnumber_to_span = ScalarFunction(
        "valueSpan",
        {TInt::TIntMake()},
        SpanType::SPAN(),
        TemporalFunctions::TnumberToSpan
    );
    ExtensionUtil::RegisterFunction(instance, tnumber_to_span);

    auto valueset = ScalarFunction(
        "valueSet",
        {TInt::TIntMake()},
        SetTypes::INTSET(),
        TemporalFunctions::TemporalValueset
    );
    ExtensionUtil::RegisterFunction(instance, valueset);

    auto sequences = ScalarFunction(
        "sequences",
        {TInt::TIntMake()},
        LogicalType::LIST(TInt::TIntMake()),
        TemporalFunctions::TemporalSequences
    );
    ExtensionUtil::RegisterFunction(instance, sequences);

    auto shift_value = ScalarFunction(
        "shiftValue",
        {TInt::TIntMake(), LogicalType::BIGINT},
        TInt::TIntMake(),
        TemporalFunctions::TnumberShiftValue
    );
    ExtensionUtil::RegisterFunction(instance, shift_value);

    auto scale_value = ScalarFunction(
        "scaleValue",
        {TInt::TIntMake(), LogicalType::BIGINT},
        TInt::TIntMake(),
        TemporalFunctions::TnumberScaleValue
    );
    ExtensionUtil::RegisterFunction(instance, scale_value);

    auto shift_scale_value = ScalarFunction(
        "shiftScaleValue",
        {TInt::TIntMake(), LogicalType::BIGINT, LogicalType::BIGINT},
        TInt::TIntMake(),
        TemporalFunctions::TnumberShiftScaleValue
    );
    ExtensionUtil::RegisterFunction(instance, shift_scale_value);
}

} // namespace duckdb