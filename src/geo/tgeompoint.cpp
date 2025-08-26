#include "meos_wrapper_simple.hpp"

#include "common.hpp"
#include "geo/tgeompoint.hpp"
#include "geo/tgeompoint_functions.hpp"
#include "temporal/temporal_functions.hpp"
#include "geo/stbox.hpp"
#include "temporal/spanset.hpp"
#include "temporal/temporal.hpp"

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
    ExtensionLoader::RegisterType(instance, "TGEOMPOINT", TGEOMPOINT());
}

void TgeompointType::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionLoader::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        TGEOMPOINT(),
        TgeompointFunctions::Tpoint_in
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        TGEOMPOINT(),
        LogicalType::VARCHAR,
        TemporalFunctions::Temporal_out
    );

    ExtensionLoader::RegisterCastFunction(
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

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "asText",
            {TGEOMPOINT()},
            LogicalType::VARCHAR,
            TgeompointFunctions::Tspatial_as_text
        )
    );

    ExtensionLoader::RegisterFunction(
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
    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "TGEOMPOINT",
            {WKB_BLOB(), LogicalType::TIMESTAMP_TZ},
            TGEOMPOINT(),
            TgeompointFunctions::Tpointinst_constructor
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tgeompointSeq",
            {LogicalType::LIST(TGEOMPOINT())},
            TGEOMPOINT(),
            TemporalFunctions::Tsequence_constructor
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "stbox",
            {TGEOMPOINT()},
            StboxType::STBOX(),
            TgeompointFunctions::Tspatial_to_stbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "getTime",
            {TGEOMPOINT()},
            SpansetTypes::tstzspanset(),
            TemporalFunctions::Temporal_time
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "startValue",
            {TGEOMPOINT()},
            WKB_BLOB(),
            TgeompointFunctions::Tgeompoint_start_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "duration",
            {TGEOMPOINT(), LogicalType::BOOLEAN},
            LogicalType::INTERVAL,
            TemporalFunctions::Temporal_duration
        )
    );

    ExtensionLoader::RegisterFunction(
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

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "atValues",
            {TGEOMPOINT(), WKB_BLOB()},
            TGEOMPOINT(),
            TgeompointFunctions::Tgeompoint_at_value
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "atTime",
            {TGEOMPOINT(), SpanTypes::TSTZSPAN()},
            TGEOMPOINT(),
            TemporalFunctions::Temporal_at_tstzspan
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "atTime",
            {TGEOMPOINT(), SpansetTypes::tstzspanset()},
            TGEOMPOINT(),
            TemporalFunctions::Temporal_at_tstzspanset
        )
    );

    ExtensionLoader::RegisterFunction(
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
    
    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "length",
            {TGEOMPOINT()},
            LogicalType::DOUBLE,
            TgeompointFunctions::Tpoint_length
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "trajectory",
            {TGEOMPOINT()},
            // WKB_BLOB(),
            LogicalType::VARCHAR,
            TgeompointFunctions::Tpoint_trajectory
        )
    );

    /* ***************************************************
     * Spatial relationships
     ****************************************************/
    
    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "aDisjoint",
            {TGEOMPOINT(), TGEOMPOINT()},
            LogicalType::BOOLEAN,
            TgeompointFunctions::Adisjoint_tgeo_tgeo
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "eDwithin",
            {TGEOMPOINT(), TGEOMPOINT(), LogicalType::DOUBLE},
            LogicalType::BOOLEAN,
            TgeompointFunctions::Edwithin_tgeo_tgeo
        )
    );

    /* ***************************************************
     * Temporal-spatial relationships
     ****************************************************/

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "tDwithin",
            {TGEOMPOINT(), TGEOMPOINT(), LogicalType::DOUBLE},
            TemporalTypes::TBOOL(),
            TgeompointFunctions::Tdwithin_tgeo_tgeo
        )
    );

    /* ***************************************************
     * Operators (workaround as functions)
     ****************************************************/

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "&&", // overlaps
            {TGEOMPOINT(), StboxType::STBOX()},
            LogicalType::BOOLEAN,
            TgeompointFunctions::Temporal_overlaps_tgeompoint_stbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "&&", // overlaps
            {TGEOMPOINT(), SpanTypes::TSTZSPAN()},
            LogicalType::BOOLEAN,
            TgeompointFunctions::Temporal_overlaps_tgeompoint_tstzspan
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "@>", // contains
            {TGEOMPOINT(), StboxType::STBOX()},
            LogicalType::BOOLEAN,
            TgeompointFunctions::Temporal_contains_tgeompoint_stbox
        )
    );
}

} // namespace duckdb