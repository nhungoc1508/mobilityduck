#pragma once

#include "meos_wrapper_simple.hpp"
#include "duckdb/common/typedefs.hpp"

#include "span.hpp"
#include "set.hpp"

namespace duckdb {

class ExtensionLoader;

#define OUT_DEFAULT_DECIMAL_DIGITS 15
#define Float8GetDatum(X) ((Datum) *(uint64_t *) &(X))
#define Int32GetDatum(X) ((Datum) (X))

struct TboxFunctions {
    // Cast functions
    static bool Tbox_in(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
    static bool Tbox_out(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

    // Constructor functions
    static void Number_timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Numspan_timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Number_tstzspan_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);;
    static void Numspan_tstzspan_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);

    // Conversion functions
    static void Number_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Numeric_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Set_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Span_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
    static void Spanset_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb