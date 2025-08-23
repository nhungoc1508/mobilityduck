#include "meos_wrapper_simple.hpp"

#include "common.hpp"
#include "geo/tgeompoint.hpp"
#include "geo/tgeompoint_functions.hpp"
#include "temporal/temporal_functions.hpp"
#include "geo/stbox.hpp"
#include "spanset.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

LogicalType TgeompointType::TGEOMPOINT() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("TGEOMPOINT");
    return type;
}

LogicalType TgeompointType::WKB_BLOB() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("WKB_BLOB");
    return type;
}

void TgeompointType::RegisterType(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TGEOMPOINT", TGEOMPOINT());
}

void TgeompointType::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionUtil::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TGEOMPOINT(),
        TgeompointFunctions::Tpoint_in
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TGEOMPOINT(),
        LogicalType::VARCHAR,
        TemporalFunctions::Temporal_out
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TGEOMPOINT(),
        StboxType::STBOX(),
        TgeompointFunctions::Tspatial_to_stbox_cast
    );
}

void TgeompointType::RegisterScalarFunctions(DatabaseInstance &instance) {

    /* ***************************************************
     * In/out functions
     ****************************************************/

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "asText",
            {TGEOMPOINT()},
            LogicalType::VARCHAR,
            TgeompointFunctions::Tspatial_as_text
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "asEWKT",
            {TGEOMPOINT()},
            LogicalType::VARCHAR,
            TgeompointFunctions::Tspatial_as_ewkt
        )
    );

    /* ***************************************************
    * Constructor functions
    ****************************************************/
    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "TGEOMPOINT",
            {WKB_BLOB(), LogicalType::TIMESTAMP_TZ},
            TGEOMPOINT(),
            TgeompointFunctions::Tpointinst_constructor
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "tgeompointSeq",
            {LogicalType::LIST(TGEOMPOINT())},
            TGEOMPOINT(),
            TemporalFunctions::Tsequence_constructor
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "stbox",
            {TGEOMPOINT()},
            StboxType::STBOX(),
            TgeompointFunctions::Tspatial_to_stbox
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "getTime",
            {TGEOMPOINT()},
            SpansetTypes::tstzspanset(),
            TemporalFunctions::Temporal_time
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "startValue",
            {TGEOMPOINT()},
            WKB_BLOB(),
            TgeompointFunctions::Tgeompoint_start_value
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "duration",
            {TGEOMPOINT(), LogicalType::BOOLEAN},
            LogicalType::INTERVAL,
            TemporalFunctions::Temporal_duration
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "startTimestamp",
            {TGEOMPOINT()},
            LogicalType::TIMESTAMP_TZ,
            TemporalFunctions::Temporal_start_timestamptz
        )
    );

    /* ***************************************************
     * Restriction functions
     ****************************************************/

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "atValues",
            {TGEOMPOINT(), WKB_BLOB()},
            TGEOMPOINT(),
            TgeompointFunctions::Tgeompoint_at_value
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "atTime",
            {TGEOMPOINT(), SpanTypes::TSTZSPAN()},
            TGEOMPOINT(),
            TemporalFunctions::Temporal_at_tstzspan
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "valueAtTimestamp",
            {TGEOMPOINT(), LogicalType::TIMESTAMP_TZ},
            WKB_BLOB(),
            TgeompointFunctions::Tgeompoint_value_at_timestamptz
        )
    );

    /* ***************************************************
     * Spatial functions
     ****************************************************/
    
    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "length",
            {TGEOMPOINT()},
            LogicalType::DOUBLE,
            TgeompointFunctions::Tpoint_length
        )
    );

    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "trajectory",
            {TGEOMPOINT()},
            WKB_BLOB(),
            TgeompointFunctions::Tpoint_trajectory
        )
    );

    /* ***************************************************
     * Spatial relationships
     ****************************************************/
    
    ExtensionUtil::RegisterFunction(
        instance,
        ScalarFunction(
            "aDisjoint",
            {TGEOMPOINT(), TGEOMPOINT()},
            LogicalType::BOOLEAN,
            TgeompointFunctions::Adisjoint_tgeo_tgeo
        )
    );
}

} // namespace duckdb