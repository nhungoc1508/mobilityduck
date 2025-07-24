#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class ExtensionLoader;

struct GeoFunctions {
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

} // namespace duckdb