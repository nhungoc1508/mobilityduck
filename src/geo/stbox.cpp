#include "meos_wrapper_simple.hpp"

#include "common.hpp"
#include "geo/stbox.hpp"
#include "geo/stbox_functions.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

LogicalType StboxType::STBOX() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("STBOX");
    return type;
}

LogicalType StboxType::WKB_BLOB() {
    LogicalType type(LogicalTypeId::BLOB);
    type.SetAlias("WKB_BLOB");
    return type;
}

void StboxType::RegisterType(DatabaseInstance &instance) {
    ExtensionLoader::RegisterType(instance, "STBOX", STBOX());
}

void StboxType::RegisterCastFunctions(DatabaseInstance &instance) {
    ExtensionLoader::RegisterCastFunction(
        instance,
        LogicalType::VARCHAR,
        STBOX(),
        StboxFunctions::Stbox_in_cast
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        STBOX(),
        LogicalType::VARCHAR,
        StboxFunctions::Stbox_out
    );

    ExtensionLoader::RegisterCastFunction(
        instance,
        WKB_BLOB(),
        STBOX(),
        StboxFunctions::Geo_to_stbox_cast
    );
}

void StboxType::RegisterScalarFunctions(DatabaseInstance &instance) {
    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "stbox",
            {LogicalType::VARCHAR},
            STBOX(),
            StboxFunctions::Stbox_in
        )
    );
    
    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "stboxFromBinary",
            {LogicalType::BLOB},
            STBOX(),
            StboxFunctions::Stbox_from_wkb
        )
    );

    // ExtensionLoader::RegisterFunction(
    //     instance,
    //     ScalarFunction(
    //         "stboxFromHexWKB",
    //         {LogicalType::VARCHAR},
    //         STBOX(),
    //         StboxFunctions::Stbox_from_hexwkb
    //     )
    // );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "asText",
            {STBOX()},
            LogicalType::VARCHAR,
            StboxFunctions::Stbox_as_text
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "asBinary",
            {STBOX()},
            LogicalType::BLOB,
            StboxFunctions::Stbox_as_wkb
        )
    );

    // ExtensionLoader::RegisterFunction(
    //     instance,
    //     ScalarFunction(
    //         "asHexWKB",
    //         {STBOX()},
    //         LogicalType::VARCHAR,
    //         StboxFunctions::Stbox_as_hexwkb
    //     )
    // );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "stbox",
            {StboxType::WKB_BLOB(), LogicalType::TIMESTAMP_TZ},
            StboxType::STBOX(),
            StboxFunctions::Geo_timestamptz_to_stbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "stbox",
            {StboxType::WKB_BLOB(), SpanTypes::TSTZSPAN()},
            StboxType::STBOX(),
            StboxFunctions::Geo_tstzspan_to_stbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "stbox",
            {StboxType::WKB_BLOB()},
            StboxType::STBOX(),
            StboxFunctions::Geo_to_stbox
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "expandSpace",
            {STBOX(), LogicalType::DOUBLE},
            STBOX(),
            StboxFunctions::Stbox_expand_space
        )
    );

    ExtensionLoader::RegisterFunction(
        instance,
        ScalarFunction(
            "&&",
            {STBOX(), STBOX()},
            LogicalType::BOOLEAN,
            StboxFunctions::Overlaps_stbox_stbox
        )
    );
}

} // namespace duckdb