#pragma once

#include "common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "properties.hpp"

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>    
}

namespace duckdb {

// Define the MEOS temptype for tint (should match meosType enum)
// Removed #define T_TINT 6 to avoid conflict with meos_catalog.h

enum class TemporalGeometryType: uint8_t {
    TGEOMPOINT = 0,
    TGEOGPOINT
};

struct TemporalGeometryTypes {
    static LogicalType TGEOMPOINT();
    static LogicalType TGEOGPOINT();

    static string ToString(TemporalGeometryType type) {
        switch (type) {
        case TemporalGeometryType::TGEOMPOINT:
            return "TGEOMPOINT";
        case TemporalGeometryType::TGEOGPOINT:
            return "TGEOGPOINT";
        default:
            return StringUtil::Format("Unknown (%d)", static_cast<int>(type));
        }
    }
};

class geometry_t {
private:
    string_t data;

public:
    geometry_t() = default;

    explicit geometry_t(string_t data) : data(data) {}

    operator string_t() const {
        return data;
    }

    TemporalGeometryType GetType() const {
        const auto type = Load<TemporalGeometryType>(const_data_ptr_cast(data.GetPrefix()));
        return type;
    }

    TemporalGeometryProperties GetProperties() const {
        const auto props = Load<TemporalGeometryProperties>(const_data_ptr_cast(data.GetPrefix() + 1));
        return props;
    }
};

class ExtensionLoader;

struct GeoTypes {
    static LogicalType TINSTANT();
    static LogicalType TINT();
    static void RegisterTypes(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

struct TypeMapping {
    static meosType GetMeosTypeFromAlias(const std::string &alias);
    static LogicalType GetChildType(const LogicalType &type);
};

} // namespace duckdb