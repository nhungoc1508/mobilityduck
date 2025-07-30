#define MOBILITYDUCK_TEMPORAL_TYPES

#include "common.hpp"
#include "temporal/temporal_types.hpp"
#include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

void TemporalTypes::RegisterTypes(DatabaseInstance &instance) {
    TInt::RegisterType(instance);
    TBool::RegisterType(instance);
}

void TemporalTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    TInt::RegisterCastFunctions(instance);
    TBool::RegisterCastFunctions(instance);
}

void TemporalTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    TInt::RegisterScalarFunctions(instance);
    TBool::RegisterScalarFunctions(instance);
}

} // namespace duckdb

#ifndef MOBILITYDUCK_TEMPORAL_TYPES
#endif