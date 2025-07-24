#define MOBILITYDUCK_EXTENSION_TYPES

#include "common.hpp"
#include "types.hpp"
#include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

LogicalType GeoTypes::TINSTANT() {
    auto type = LogicalType::STRUCT({
        {"value", LogicalType::BIGINT},  // Using BIGINT for Datum for now
        {"temptype", LogicalType::UTINYINT},
        {"t", LogicalType::TIMESTAMP_TZ}});
    type.SetAlias("TINSTANT");
    return type;
}

LogicalType GeoTypes::TINT() {
    auto type = LogicalType::STRUCT({
        {"value", LogicalType::BIGINT},
        {"temptype", LogicalType::UTINYINT},
        {"t", LogicalType::TIMESTAMP_TZ}});
    type.SetAlias("TINT");
    return type;
}

void GeoTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINSTANT", GeoTypes::TINSTANT());
    ExtensionUtil::RegisterType(instance, "TINT", GeoTypes::TINT());
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif