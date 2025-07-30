#define MOBILITYDUCK_EXTENSION_TYPES

#include "common.hpp"
#include "types.hpp"
#include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

LogicalType GeoTypes::TInstantType() {
    return LogicalType::STRUCT({
        {"value", LogicalType::BIGINT},
        {"temptype", LogicalType::UTINYINT},
        {"t", LogicalType::TIMESTAMP_TZ}
    });
}

LogicalType GeoTypes::TSequenceType() {
    return LogicalType::STRUCT({
        {"instants", LogicalType::LIST(GeoTypes::TInstantType())},
        {"interp", LogicalType::UTINYINT},
        {"lower_inc", LogicalType::BOOLEAN},
        {"upper_inc", LogicalType::BOOLEAN}
    });
}

LogicalType GeoTypes::TSequenceSetType() {
    return LogicalType::STRUCT({
        {"sequences", LogicalType::LIST(GeoTypes::TSequenceType())}
    });
}

LogicalType GeoTypes::TINT() {
    auto type = LogicalType::STRUCT({
        {"subtype", LogicalType::UTINYINT},
        {"instant", GeoTypes::TInstantType()},
        {"sequence", GeoTypes::TSequenceType()},
        {"sequenceset", GeoTypes::TSequenceSetType()}
    });
    type.SetAlias("TINT");
    return type;
}

LogicalType GeoTypes::TINSTANT() {
    auto type = LogicalType::STRUCT({
        {"value", LogicalType::BIGINT},  // Using BIGINT for Datum for now
        {"temptype", LogicalType::UTINYINT},
        {"t", LogicalType::TIMESTAMP_TZ}});
    type.SetAlias("TINSTANT");
    return type;
}

// LogicalType GeoTypes::TINT() {
//     auto type = LogicalType::STRUCT({
//         {"value", LogicalType::BIGINT},
//         {"temptype", LogicalType::UTINYINT},
//         {"t", LogicalType::TIMESTAMP_TZ}});
//     type.SetAlias("TINT");
//     return type;
// }

void GeoTypes::RegisterTypes(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "TINSTANT", GeoTypes::TINSTANT());
    ExtensionUtil::RegisterType(instance, "TINT", GeoTypes::TINT());
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif