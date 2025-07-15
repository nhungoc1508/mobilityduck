#define MOBILITYDUCK_EXTENSION_TYPES

#include "common.hpp"
#include "types.hpp"
#include "duckdb/common/extension_type_info.hpp"

namespace duckdb {

LogicalType GeoTypes::POINT_2D() {
    auto type = LogicalType::STRUCT({{"x", LogicalType::DOUBLE}, {"y", LogicalType::DOUBLE}});
    type.SetAlias("POINT_2D");
    return type;
}

LogicalType GeoTypes::TPOINT_2D() {
    auto type = LogicalType::STRUCT({
        {"x", LogicalType::DOUBLE},
        {"y", LogicalType::DOUBLE},
        {"t", LogicalType::TIMESTAMP_TZ}});
    type.SetAlias("TPOINT_2D");
    return type;
}

inline void ExecutePoint2D(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &x = args.data[0];
    auto &y = args.data[1];
    x.Flatten(count);
    y.Flatten(count);

    auto &children = StructVector::GetEntries(result);
    auto &x_child = children[0];
    auto &y_child = children[1];
    x_child->Reference(x);
    y_child->Reference(y);

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

inline void ExecuteTPoint2D(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &x = args.data[0];
    auto &y = args.data[1];
    auto &t = args.data[2];
    x.Flatten(count);
    y.Flatten(count);
    t.Flatten(count);

    auto &children = StructVector::GetEntries(result);
    auto &x_child = children[0];
    auto &y_child = children[1];
    auto &t_child = children[2];
    x_child->Reference(x);
    y_child->Reference(y);
    t_child->Reference(t);

    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void GeoTypes::RegisterScalarFunctions(DatabaseInstance &instance) {
    // Register ST_Point2D function
    auto point2d_scalar_function = ScalarFunction(
        "ST_Point2D", // name
        {LogicalType::DOUBLE, LogicalType::DOUBLE}, // arguments
        GeoTypes::POINT_2D(), // return type
        ExecutePoint2D); // function
    ExtensionUtil::RegisterFunction(instance, point2d_scalar_function);

    // Register ST_TPoint2D function
    auto tpoint2d_scalar_function = ScalarFunction(
        "ST_TPoint2D", // name
        {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::TIMESTAMP_TZ}, // arguments
        GeoTypes::TPOINT_2D(), // return type
        ExecuteTPoint2D); // function
    ExtensionUtil::RegisterFunction(instance, tpoint2d_scalar_function);
}

void GeoTypes::Register(DatabaseInstance &instance) {
    ExtensionUtil::RegisterType(instance, "POINT_2D", GeoTypes::POINT_2D());
    ExtensionUtil::RegisterType(instance, "TPOINT_2D", GeoTypes::TPOINT_2D());
}

} // namespace duckdb

#ifndef MOBILITYDUCK_EXTENSION_TYPES
#endif