#pragma once

#include "common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "properties.hpp"

namespace duckdb {

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
    static LogicalType POINT_2D();
    static LogicalType TPOINT_2D();
    static void Register(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

} // namespace duckdb