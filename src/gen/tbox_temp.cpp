#include "meos_wrapper_simple.hpp"

#include "common.hpp"
#include "gen/tbox_temp.hpp"
#include "gen/tbox_temp_functions.hpp"

#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

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
        TboxFunctions::Tbox_in_cast
    );

    ExtensionUtil::RegisterCastFunction(
        instance,
        TBOX(),
        LogicalType::VARCHAR,
        TboxFunctions::Tbox_out_cast
    );
}

} // namespace duckdb