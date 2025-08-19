#pragma once

#include "meos_wrapper_simple.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

class ExtensionLoader;

struct TboxFunctions {
	static void Tbox_in(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Tbox_in_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Tbox_out(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Tbox_out_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Tbox_from_wkb(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_from_hexwkb(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_as_text(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_as_wkb(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_as_hexwkb(DataChunk &args, ExpressionState &state, Vector &result);
	static void Number_timestamptz_to_tbox_integer_timestamptz(DataChunk &args, ExpressionState &state, Vector &result);
	static void Number_timestamptz_to_tbox_float_timestamptz(DataChunk &args, ExpressionState &state, Vector &result);
	static void Numspan_timestamptz_to_tbox_intspan_timestamptz(DataChunk &args, ExpressionState &state, Vector &result);
	static void Numspan_timestamptz_to_tbox_floatspan_timestamptz(DataChunk &args, ExpressionState &state, Vector &result);
	static void Number_tstzspan_to_tbox_integer_tstzspan(DataChunk &args, ExpressionState &state, Vector &result);
	static void Number_tstzspan_to_tbox_float_tstzspan(DataChunk &args, ExpressionState &state, Vector &result);
	static void Numspan_tstzspan_to_tbox_intspan_tstzspan(DataChunk &args, ExpressionState &state, Vector &result);
	static void Numspan_tstzspan_to_tbox_floatspan_tstzspan(DataChunk &args, ExpressionState &state, Vector &result);
	static void Number_to_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Number_to_tbox_integer_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Number_to_tbox_float(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Number_to_tbox_float_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Timestamptz_to_tbox_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Set_to_tbox_intset(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Set_to_tbox_intset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Set_to_tbox_floatset(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Set_to_tbox_floatset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Set_to_tbox_tstzset(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Set_to_tbox_tstzset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Span_to_tbox_intspan(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Span_to_tbox_intspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Span_to_tbox_floatspan(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Span_to_tbox_floatspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Span_to_tbox_tstzspan(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Span_to_tbox_tstzspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Spanset_to_tbox_intspanset(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Spanset_to_tbox_intspanset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Spanset_to_tbox_floatspanset(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Spanset_to_tbox_floatspanset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Spanset_to_tbox_tstzspanset(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Spanset_to_tbox_tstzspanset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Tbox_to_intspan(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Tbox_to_intspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Tbox_to_floatspan(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Tbox_to_floatspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Tbox_to_tstzspan(DataChunk &args, ExpressionState &state, Vector &result);
	static bool Tbox_to_tstzspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters);
	static void Tbox_hasx(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_hast(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_xmin(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_xmin_inc(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_xmax(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_xmax_inc(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_tmin(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_tmin_inc(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_tmax(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_tmax_inc(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_shift_value_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_shift_value_tbox_float(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_shift_time(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_scale_value_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_scale_value_tbox_float(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_scale_time(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_shift_scale_value_tbox_integer_integer(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_shift_scale_value_tbox_float_float(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_shift_scale_time(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_expand_value_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_expand_value_tbox_float(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_expand_time(DataChunk &args, ExpressionState &state, Vector &result);
	static void Tbox_round(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb