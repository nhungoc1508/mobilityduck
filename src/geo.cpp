#include "geo.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"


extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>        
}

namespace duckdb {

#define DEFINE_SPATIAL_TYPE(NAME)                                   \
    LogicalType Spatial::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::VARCHAR);            \
        type.SetAlias(#NAME);                                       \
        return type;                                                \
    }

DEFINE_SPATIAL_TYPE(Point)
DEFINE_SPATIAL_TYPE(Line)
DEFINE_SPATIAL_TYPE(Polygon)
DEFINE_SPATIAL_TYPE(Path)
DEFINE_SPATIAL_TYPE(Box)
DEFINE_SPATIAL_TYPE(Circle)
DEFINE_SPATIAL_TYPE(Geometry)
DEFINE_SPATIAL_TYPE(Geography)

#undef DEFINE_SPATIAL_TYPE

void Spatial::RegisterSpatialType(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "Point", Point());
    ExtensionUtil::RegisterType(db, "Line", Line());
    ExtensionUtil::RegisterType(db, "Polygon", Polygon());
    ExtensionUtil::RegisterType(db, "Path", Path());
    ExtensionUtil::RegisterType(db, "Box", Box());
    ExtensionUtil::RegisterType(db, "Circle", Circle());
    ExtensionUtil::RegisterType(db, "Geometry", Geometry());
    ExtensionUtil::RegisterType(db, "Geography", Geography());    
}    

struct Point2D {
    double x;
    double y;
};

void PointConstructor(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<double, double, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](double x, double y) -> string_t {
            Point2D pt{x, y};

            std::ostringstream ss;
            ss << "(" << pt.x << "," << pt.y << ")";
            return StringVector::AddString(result, ss.str());
        }
    );
}

void Spatial::RegisterPointConstructor(DatabaseInstance &db) {       
    ExtensionUtil::RegisterFunction(db, 
    ScalarFunction (
        "Point",
        {LogicalType::DOUBLE, LogicalType::DOUBLE},
        Spatial::Point(),
        PointConstructor  
    ));
}

}