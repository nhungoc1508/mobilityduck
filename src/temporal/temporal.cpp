#include "meos_wrapper_simple.hpp"

#include "common.hpp"
#include "temporal/temporal.hpp"
#include "temporal/temporal_functions.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

#define DEFINE_TEMPORAL_TYPE(NAME) \
    LogicalType TemporalTypes::NAME() { \
        LogicalType type(LogicalTypeId::BLOB); \
        type.SetAlias(#NAME); \
        return type; \
    }

DEFINE_TEMPORAL_TYPE(TINT)
DEFINE_TEMPORAL_TYPE(TBOOL)
DEFINE_TEMPORAL_TYPE(TFLOAT)
DEFINE_TEMPORAL_TYPE(TTEXT)

#undef DEFINE_TEMPORAL_TYPE

void TemporalTypes::RegisterTypes(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "TINT", TINT());
    ExtensionUtil::RegisterType(db, "TBOOL", TBOOL());
    ExtensionUtil::RegisterType(db, "TFLOAT", TFLOAT());
    ExtensionUtil::RegisterType(db, "TTEXT", TTEXT());
}

const std::vector<LogicalType> &TemporalTypes::AllTypes() {
    static std::vector<LogicalType> types = {
        TINT(),
        TBOOL(),
        TFLOAT(),
        TTEXT()
    };
    return types;
}

LogicalType TemporalTypes::GetBaseTypeFromAlias(const char *alias) {
    for (size_t i = 0; i < sizeof(BASE_TYPES) / sizeof(BASE_TYPES[0]); i++) {
        if (strcmp(alias, BASE_TYPES[i].alias) == 0) {
            return BASE_TYPES[i].basetype;
        }
    }
    throw InternalException("Invalid temporal type alias: %s", alias);
}

void TemporalTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    for (auto &type : TemporalTypes::AllTypes()) {
        ExtensionUtil::RegisterCastFunction(
            instance,
            LogicalType::VARCHAR,
            type,
            TemporalFunctions::StringToTemporal
        );

        ExtensionUtil::RegisterCastFunction(
            instance,
            type,
            LogicalType::VARCHAR,
            TemporalFunctions::TemporalToString
        );
    }
}

void TemporalTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    for (auto &type : TemporalTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()),
                {TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()), LogicalType::TIMESTAMP_TZ},
                type,
                TemporalFunctions::TInstantConstructor
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "tempSubtype",
                {type},
                LogicalType::VARCHAR,
                TemporalFunctions::TemporalSubtype
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "interp",
                {type},
                LogicalType::VARCHAR,
                TemporalFunctions::TemporalInterp
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "getValue",
                {type},
                TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()),
                TemporalFunctions::TInstantValue
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "startValue",
                {type},
                TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()),
                TemporalFunctions::TemporalStartValue
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "endValue",
                {type},
                TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()),
                TemporalFunctions::TemporalEndValue
            )
        );

        if (type.GetAlias() != "TBOOL") {
            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "minValue",
                    {type},
                    TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()),
                    TemporalFunctions::TemporalMinValue
                )
            );

            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "maxValue",
                    {type},
                    TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()),
                    TemporalFunctions::TemporalMaxValue
                )
            );

            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "minInstant",
                    {type},
                    type,
                    TemporalFunctions::TemporalMinInstant
                )
            );
    
            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "maxInstant",
                    {type},
                    type,
                    TemporalFunctions::TemporalMaxInstant
                )
            );
        }

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "valueN",
                {type, LogicalType::BIGINT},
                TemporalTypes::GetBaseTypeFromAlias(type.GetAlias().c_str()),
                TemporalFunctions::TemporalValueN
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "getTimestamp",
                {type},
                LogicalType::TIMESTAMP_TZ,
                TemporalFunctions::TInstantTimestamptz
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "duration",
                {type, LogicalType::BOOLEAN},
                LogicalType::INTERVAL,
                TemporalFunctions::TemporalDuration
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "Seq",
                {LogicalType::LIST(type)},
                type,
                TemporalFunctions::TsequenceConstructor
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "Seq",
                {LogicalType::LIST(type), LogicalType::VARCHAR},
                type,
                TemporalFunctions::TsequenceConstructor
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "Seq",
                {LogicalType::LIST(type), LogicalType::VARCHAR, LogicalType::BOOLEAN},
                type,
                TemporalFunctions::TsequenceConstructor
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "Seq",
                {LogicalType::LIST(type), LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
                type,
                TemporalFunctions::TsequenceConstructor
            )
        );
        
        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "Seq",
                {type, LogicalType::VARCHAR},
                type,
                TemporalFunctions::TemporalToTsequence
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "Seq",
                {type},
                type,
                TemporalFunctions::TemporalToTsequence
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "SeqSet",
                {LogicalType::LIST(type)},
                type,
                TemporalFunctions::TsequencesetConstructor
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                StringUtil::Lower(type.GetAlias()) + "SeqSet",
                {type},
                type,
                TemporalFunctions::TemporalToTsequenceset
            )
        );

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "timeSpan",
                {type},
                SpanTypes::TSTZSPAN(),
                TemporalFunctions::TemporalToTstzspan
            )
        );

        if (type.GetAlias() == "TINT") {
            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "valueSpan",
                    {type},
                    SpanTypes::INTSPAN(),
                    TemporalFunctions::TnumberToSpan
                )
            );

            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "valueSet",
                    {type},
                    SetTypes::INTSET(),
                    TemporalFunctions::TemporalValueset
                )
            );
        } else if (type.GetAlias() == "TFLOAT") {
            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "valueSpan",
                    {type},
                    SpanTypes::FLOATSPAN(),
                    TemporalFunctions::TnumberToSpan
                )
            );

            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "valueSet",
                    {type},
                    SetTypes::FLOATSET(),
                    TemporalFunctions::TemporalValueset
                )
            );
        }

        ExtensionUtil::RegisterFunction(
            instance,
            ScalarFunction(
                "sequences",
                {type},
                LogicalType::LIST(type),
                TemporalFunctions::TemporalSequences
            )
        );

        if (type.GetAlias() == "TINT" || type.GetAlias() == "TFLOAT") {
            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "shiftValue",
                    {type, LogicalType::BIGINT},
                    type,
                    TemporalFunctions::TnumberShiftValue
                )
            );

            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "scaleValue",
                    {type, LogicalType::BIGINT},
                    type,
                    TemporalFunctions::TnumberScaleValue
                )
            );

            ExtensionUtil::RegisterFunction(
                instance,
                ScalarFunction(
                    "shiftScaleValue",
                    {type, LogicalType::BIGINT, LogicalType::BIGINT},
                    type,
                    TemporalFunctions::TnumberShiftScaleValue
                )
            );
        }
    }
}

} // namespace duckdb