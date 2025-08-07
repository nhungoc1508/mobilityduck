#include "meos_wrapper_simple.hpp"
#include "common.hpp"

#include "temporal/tbox_functions.hpp"
#include "time_util.hpp"

// #include "duckdb/common/types/blob.hpp"
#include "duckdb/common/exception.hpp"
// #include "duckdb/common/string_util.hpp"
// #include "duckdb/function/scalar_function.hpp"
// #include "duckdb/main/extension_util.hpp"
// #include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

bool TboxFunctions::Tbox_in(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
            source, result, count,
            [&](string_t input, ValidityMask &mask, idx_t idx) {
                if (input.GetSize() == 0) {
                    return string_t();
                }
                std::string input_str(input.GetDataUnsafe(), input.GetSize());
                TBox *tbox = tbox_in(input_str.c_str());
                if (!tbox) {
                    throw InternalException("Failure in Tbox_in: unable to cast string to tbox");
                    success = false;
                    return string_t();
                }
                size_t tbox_size = sizeof(TBox);
                char *tbox_data = (char*)malloc(tbox_size);
                memcpy(tbox_data, tbox, tbox_size);
                return string_t(tbox_data, tbox_size);
            }
        );
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

bool TboxFunctions::Tbox_out(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input) {
            TBox *tbox = nullptr;
            if (input.GetSize() > 0) {
                tbox = (TBox*)malloc(input.GetSize());
                memcpy(tbox, input.GetDataUnsafe(), input.GetSize());
            }
            if (!tbox) {
                throw InternalException("Failure in Tbox_out: unable to cast binary to tbox");
                success = false;
                return string_t();
            }
            char *str = tbox_out(tbox, OUT_DEFAULT_DECIMAL_DIGITS);
            return string_t(str);
        }
    );
    return success;
}

template <typename TA>
static void NumberTimestamptzToTboxExecutor(Vector &value, Vector &t, meosType basetype, Vector &result, idx_t count) {
    BinaryExecutor::Execute<TA, timestamp_tz_t, string_t>(
        value, t, result, count,
        [&](TA value, timestamp_tz_t t) {
            timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(t);
            Datum datum;
            if (basetype == T_INT4) {
                datum = Int32GetDatum(value);
            } else if (basetype == T_FLOAT8) {
                datum = Float8GetDatum(value);
            } else {
                throw InternalException("Unsupported basetype in NumberTimestamptzToTboxExecutor");
            }
            TBox *tbox = number_timestamptz_to_tbox(datum, basetype, (TimestampTz)meos_ts);
            size_t tbox_size = sizeof(TBox);
            char *tbox_data = (char*)malloc(tbox_size);
            memcpy(tbox_data, tbox, tbox_size);
            free(tbox);
            return string_t(tbox_data, tbox_size);
        }
    );
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Number_timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result) {
    const auto &arg_type = args.data[0].GetType();
    
    if (arg_type.id() == LogicalTypeId::INTEGER) {
        NumberTimestamptzToTboxExecutor<int64_t>(args.data[0], args.data[1], T_INT4, result, args.size());
    } else if (arg_type.id() == LogicalTypeId::DOUBLE) {
        NumberTimestamptzToTboxExecutor<double>(args.data[0], args.data[1], T_FLOAT8, result, args.size());
    } else {
        throw InternalException("Number_timestamptz_to_tbox: args[0] must be integer or float");
    }
}

void TboxFunctions::Numspan_timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, timestamp_tz_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t span_str, timestamp_tz_t t) {
            Span *span = nullptr;
            if (span_str.GetSize() > 0) {
                span = (Span*)malloc(span_str.GetSize());
                memcpy(span, span_str.GetDataUnsafe(), span_str.GetSize());
            }
            if (!span) {
                throw InternalException("Failure in Numspan_timestamptz_to_tbox: unable to cast binary to span");
            }
            timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(t);
            TBox *tbox = numspan_timestamptz_to_tbox(span, (TimestampTz)meos_ts);
            size_t tbox_size = sizeof(TBox);
            char *tbox_data = (char*)malloc(tbox_size);
            memcpy(tbox_data, tbox, tbox_size);
            free(span);
            free(tbox);
            return string_t(tbox_data, tbox_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

template <typename TA>
static void NumberTstzspanToTboxExecutor(Vector &value, Vector &span_str, meosType basetype, Vector &result, idx_t count) {
    BinaryExecutor::Execute<TA, string_t, string_t>(
        value, span_str, result, count,
        [&](TA value, string_t span_str) {
            Datum datum;
            if (basetype == T_INT4) {
                datum = Int32GetDatum(value);
            } else if (basetype == T_FLOAT8) {
                datum = Float8GetDatum(value);
            } else {
                throw InternalException("Unsupported basetype in NumberTstzspanToTboxExecutor");
            }
            Span *span = nullptr;
            if (span_str.GetSize() > 0) {
                span = (Span*)malloc(span_str.GetSize());
                memcpy(span, span_str.GetDataUnsafe(), span_str.GetSize());
            }
            if (!span) {
                throw InternalException("Failure in NumberTstzspanToTboxExecutor: unable to cast binary to span");
            }
            TBox *tbox = number_tstzspan_to_tbox(datum, basetype, span);
            size_t tbox_size = sizeof(TBox);
            char *tbox_data = (char*)malloc(tbox_size);
            memcpy(tbox_data, tbox, tbox_size);
            free(span);
            free(tbox);
            return string_t(tbox_data, tbox_size);
        }
    );
    if (count == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Number_tstzspan_to_tbox(DataChunk &args, ExpressionState &state, Vector &result) {
    const auto &arg_type = args.data[0].GetType();

    if (arg_type.id() == LogicalTypeId::INTEGER) {
        NumberTstzspanToTboxExecutor<int64_t>(args.data[0], args.data[1], T_INT4, result, args.size());
    } else if (arg_type.id() == LogicalTypeId::DOUBLE) {
        NumberTstzspanToTboxExecutor<double>(args.data[0], args.data[1], T_FLOAT8, result, args.size());
    } else {
        throw InternalException("Number_tstzspan_to_tbox: args[0] must be integer or float");
    }
}

void TboxFunctions::Numspan_tstzspan_to_tbox(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t numspan_str, string_t tstzspan_str) {
            Span *numspan = nullptr;
            if (numspan_str.GetSize() > 0) {
                numspan = (Span*)malloc(numspan_str.GetSize());
                memcpy(numspan, numspan_str.GetDataUnsafe(), numspan_str.GetSize());
            }
            if (!numspan) {
                throw InternalException("Failure in Numspan_tstzspan_to_tbox: unable to cast binary to span");
            }

            Span *tstzspan = nullptr;
            if (tstzspan_str.GetSize() > 0) {
                tstzspan = (Span*)malloc(tstzspan_str.GetSize());
                memcpy(tstzspan, tstzspan_str.GetDataUnsafe(), tstzspan_str.GetSize());
            }
            if (!tstzspan) {
                throw InternalException("Failure in Numspan_tstzspan_to_tbox: unable to cast binary to span");
            }

            TBox *tbox = numspan_tstzspan_to_tbox(numspan, tstzspan);
            size_t tbox_size = sizeof(TBox);
            char *tbox_data = (char*)malloc(tbox_size);
            memcpy(tbox_data, tbox, tbox_size);
            free(numspan);
            free(tstzspan);
            free(tbox);
            return string_t(tbox_data, tbox_size);
        }
    );
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

} // namespace duckdb