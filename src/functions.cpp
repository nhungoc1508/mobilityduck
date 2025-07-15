#include "common.hpp"
#include "types.hpp"
#include "functions.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

// inline void ExecutePoint2D(DataChunk &args, ExpressionState &state, Vector &result) {
//     auto count = args.size();
//     auto &x = args.data[0];
//     auto &y = args.data[1];
//     x.Flatten(count);
//     y.Flatten(count);

//     auto &children = StructVector::GetEntries(result);
//     auto &x_child = children[0];
//     auto &y_child = children[1];
//     x_child->Reference(x);
//     y_child->Reference(y);

//     if (count == 1) {
//         result.SetVectorType(VectorType::CONSTANT_VECTOR);
//     }
// }

// static void LoadInternal(DatabaseInstance &instance) {
//     auto point2d_scalar_function = ScalarFunction(
//         "ST_Point2D", // name
//         {LogicalType::DOUBLE, LogicalType::DOUBLE}, // arguments
//         GeoTypes::POINT_2D(), // return type
//         ExecutePoint2D); // function
// }

} // namespace duckdb