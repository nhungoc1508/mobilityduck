#define MOBILITYDUCK_TEMPORAL_TYPES

#include "common.hpp"
#include "temporal/temporal_types.hpp"
#include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

void TemporalTypes::RegisterTypes(DatabaseInstance &instance) {
    TInt::RegisterType(instance);
    TBool::RegisterType(instance);
    TInt2::RegisterType(instance);
    TInt3::RegisterType(instance);
}

void TemporalTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    TInt::RegisterCastFunctions(instance);
    TBool::RegisterCastFunctions(instance);
    TInt2::RegisterCastFunctions(instance);
    TInt3::RegisterCastFunctions(instance);
}

void TemporalTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    TInt::RegisterScalarFunctions(instance);
    TBool::RegisterScalarFunctions(instance);
    TInt2::RegisterScalarFunctions(instance);
    TInt3::RegisterScalarFunctions(instance);
}

} // namespace duckdb

#ifndef MOBILITYDUCK_TEMPORAL_TYPES
#endif