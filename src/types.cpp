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

meosType TypeMapping::GetMeosTypeFromAlias(const std::string &alias) {
    static const std::unordered_map<std::string, meosType> alias_to_type = {
        {"INTSET", T_INTSET},
        {"BIGINTSET", T_BIGINTSET},
        {"FLOATSET", T_FLOATSET},
        {"TEXTSET", T_TEXTSET},
        {"DATESET", T_DATESET},
        {"TSTZSET", T_TSTZSET}        
        // {"GEOMSET", T_GEOMSET},
        // {"GEOGSET", T_GEOGSET},
        // {"NPOINTSET", T_NPOINTSET}
    };

    auto it = alias_to_type.find(alias);
    if (it != alias_to_type.end()) {
        return it->second;
    } else {
        return T_UNKNOWN;
    }
}

LogicalType TypeMapping::GetChildType(const LogicalType &type) {
    auto alias = type.ToString();
    if (alias == "INTSET") return LogicalType::INTEGER;
    if (alias == "BIGINTSET") return LogicalType::BIGINT;
    if (alias == "FLOATSET") return LogicalType::DOUBLE;
    if (alias == "TEXTSET") return LogicalType::VARCHAR;
    if (alias == "DATESET") return LogicalType::DATE;
    if (alias == "TSTZSET") return LogicalType::TIMESTAMP_TZ;    
    throw NotImplementedException("GetChildType: unsupported alias: " + alias);
}


} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif