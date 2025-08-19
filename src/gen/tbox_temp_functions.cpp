#include "meos_wrapper_simple.hpp"
#include "common.hpp"
#include "gen/tbox_temp_functions.hpp"
#include "time_util.hpp"
#include "type_util.hpp"
#include "tydef.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

void TboxFunctions::Tbox_in(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
			std::string input_0(args0.GetDataUnsafe(), args0.GetSize());

			const char *input = input_0.c_str();
			TBox * ret = tbox_in(input);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Tbox_in_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
			std::string input_0(args0.GetDataUnsafe(), args0.GetSize());

			const char *input = input_0.c_str();
			TBox * ret = tbox_in(input);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Tbox_out(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_out: unable to cast binary to TBox *");
            }
			string_t ret = tbox_out(box, OUT_DEFAULT_DECIMAL_DIGITS);
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Tbox_out_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_out: unable to cast binary to TBox *");
            }
			string_t ret = tbox_out(box, OUT_DEFAULT_DECIMAL_DIGITS);
		
			free(box);
			return ret;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Tbox_from_wkb(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
			string_t bytea_wkb = args0;

			uint8_t *wkb = (uint8_t *) (bytea_wkb.GetDataUnsafe());		
			TBox *ret = tbox_from_wkb(wkb, bytea_wkb.GetSize());
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

// void TboxFunctions::Tbox_from_hexwkb(DataChunk &args, ExpressionState &state, Vector &result) {
//     UnaryExecutor::Execute<string_t, string_t>(
// 		args.data[0], result, args.size(),
// 		[&](string_t args0) {
// 			string_t hexwkb_text = args0;

// 			// TODO: handle this case
//             size_t ret_size = sizeof(TBox);
//             char *ret_data = (char*)malloc(ret_size);
//             memcpy(ret_data, ret, ret_size);
//             string_t blob(ret_data, ret_size);
		
// 			free(ret);
// 			return blob;
// 		}
// 	);
//     if (args.size() == 1) {
//         result.SetVectorType(VectorType::CONSTANT_VECTOR);
//     }
// }

// void TboxFunctions::Tbox_as_text(DataChunk &args, ExpressionState &state, Vector &result) {
//     BinaryExecutor::Execute<string_t, int32_t, string_t>(
// 		args.data[0], args.data[1], result, args.size(),
// 		[&](string_t args0, int32_t args1) {
//             TBox *box = nullptr;
//             if (args0.GetSize() > 0) {
//                 box = (TBox *)malloc(args0.GetSize());
//                 memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
//             }
// 			if (!box) {
//                 throw InternalException("Failure in Tbox_as_text: unable to cast binary to TBox *");
//             }
		
// 			int32_t dbl_dig_for_wkt = args1;

// 			// TODO: handle this case
		
// 			free(box);
// 			return ret;
// 		}
// 	);
//     if (args.size() == 1) {
//         result.SetVectorType(VectorType::CONSTANT_VECTOR);
//     }
// }

// void TboxFunctions::Tbox_as_wkb(DataChunk &args, ExpressionState &state, Vector &result) {
//     BinaryExecutor::Execute<string_t, string_t, string_t>(
// 		args.data[0], args.data[1], result, args.size(),
// 		[&](string_t args0, string_t args1) {
//             Datum box = DataHelpers::getDatum<string_t>(args0);
// 			string_t ret = Datum_as_wkb(fcinfo, box, T_TBOX, false);

// 			return ret;
// 		}
// 	);
//     if (args.size() == 1) {
//         result.SetVectorType(VectorType::CONSTANT_VECTOR);
//     }
// }

// void TboxFunctions::Tbox_as_hexwkb(DataChunk &args, ExpressionState &state, Vector &result) {
//     BinaryExecutor::Execute<string_t, string_t, string_t>(
// 		args.data[0], args.data[1], result, args.size(),
// 		[&](string_t args0, string_t args1) {
//             Datum box = DataHelpers::getDatum<string_t>(args0);
// 			string_t ret = Datum_as_hexwkb(fcinfo, box, T_TBOX);

// 			return ret;
// 		}
// 	);
//     if (args.size() == 1) {
//         result.SetVectorType(VectorType::CONSTANT_VECTOR);
//     }
// }

void TboxFunctions::Number_timestamptz_to_tbox_integer_timestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<int32_t, timestamp_tz_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](int32_t args0, timestamp_tz_t args1) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
		
			timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args1);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_timestamptz_to_tbox(value, basetype, (TimestampTz)meos_ts);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Number_timestamptz_to_tbox_float_timestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<int32_t, timestamp_tz_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](int32_t args0, timestamp_tz_t args1) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
		
			timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args1);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_timestamptz_to_tbox(value, basetype, (TimestampTz)meos_ts);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Numspan_timestamptz_to_tbox_intspan_timestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, timestamp_tz_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, timestamp_tz_t args1) {
            Span *span = nullptr;
            if (args0.GetSize() > 0) {
                span = (Span *)malloc(args0.GetSize());
                memcpy(span, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!span) {
                throw InternalException("Failure in Numspan_timestamptz_to_tbox: unable to cast binary to Span *");
            }
		
			timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args1);
			TBox * ret = numspan_timestamptz_to_tbox(span, (TimestampTz)meos_ts);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(span);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Numspan_timestamptz_to_tbox_floatspan_timestamptz(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, timestamp_tz_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, timestamp_tz_t args1) {
            Span *span = nullptr;
            if (args0.GetSize() > 0) {
                span = (Span *)malloc(args0.GetSize());
                memcpy(span, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!span) {
                throw InternalException("Failure in Numspan_timestamptz_to_tbox: unable to cast binary to Span *");
            }
		
			timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args1);
			TBox * ret = numspan_timestamptz_to_tbox(span, (TimestampTz)meos_ts);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(span);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Number_tstzspan_to_tbox_integer_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<int32_t, string_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](int32_t args0, string_t args1) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
		
            Span *s = nullptr;
            if (args1.GetSize() > 0) {
                s = (Span *)malloc(args1.GetSize());
                memcpy(s, args1.GetDataUnsafe(), args1.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Number_tstzspan_to_tbox: unable to cast binary to Span *");
            }
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_tstzspan_to_tbox(value, basetype, s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Number_tstzspan_to_tbox_float_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<int32_t, string_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](int32_t args0, string_t args1) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
		
            Span *s = nullptr;
            if (args1.GetSize() > 0) {
                s = (Span *)malloc(args1.GetSize());
                memcpy(s, args1.GetDataUnsafe(), args1.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Number_tstzspan_to_tbox: unable to cast binary to Span *");
            }
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_tstzspan_to_tbox(value, basetype, s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Numspan_tstzspan_to_tbox_intspan_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, string_t args1) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Numspan_tstzspan_to_tbox: unable to cast binary to Span *");
            }
		
            Span *p = nullptr;
            if (args1.GetSize() > 0) {
                p = (Span *)malloc(args1.GetSize());
                memcpy(p, args1.GetDataUnsafe(), args1.GetSize());
            }
			if (!p) {
                throw InternalException("Failure in Numspan_tstzspan_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = numspan_tstzspan_to_tbox(s, p);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(p);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Numspan_tstzspan_to_tbox_floatspan_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, string_t args1) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Numspan_tstzspan_to_tbox: unable to cast binary to Span *");
            }
		
            Span *p = nullptr;
            if (args1.GetSize() > 0) {
                p = (Span *)malloc(args1.GetSize());
                memcpy(p, args1.GetDataUnsafe(), args1.GetSize());
            }
			if (!p) {
                throw InternalException("Failure in Numspan_tstzspan_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = numspan_tstzspan_to_tbox(s, p);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(p);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Number_to_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<int32_t, string_t>(
		args.data[0], result, args.size(),
		[&](int32_t args0) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_tbox(value, basetype);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Number_to_tbox_integer_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<int32_t, string_t>(
		source, result, count,
		[&](int32_t args0) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_tbox(value, basetype);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Number_to_tbox_float(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<int32_t, string_t>(
		args.data[0], result, args.size(),
		[&](int32_t args0) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_tbox(value, basetype);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Number_to_tbox_float_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<int32_t, string_t>(
		source, result, count,
		[&](int32_t args0) {
            Datum value = DataHelpers::getDatum<int32_t>(args0);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args0);
			TBox * ret = number_tbox(value, basetype);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Timestamptz_to_tbox(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<timestamp_tz_t, string_t>(
		args.data[0], result, args.size(),
		[&](timestamp_tz_t args0) {
			timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args0);
			TBox * ret = timestamptz_to_tbox((TimestampTz)meos_ts);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Timestamptz_to_tbox_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<timestamp_tz_t, string_t>(
		source, result, count,
		[&](timestamp_tz_t args0) {
			timestamp_tz_t meos_ts = DuckDBToMeosTimestamp(args0);
			TBox * ret = timestamptz_to_tbox((TimestampTz)meos_ts);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Set_to_tbox_intset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Set *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Set *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Set_to_tbox: unable to cast binary to Set *");
            }
			TBox * ret = set_to_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Set_to_tbox_intset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Set *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Set *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Set_to_tbox: unable to cast binary to Set *");
            }
			TBox * ret = set_to_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Set_to_tbox_floatset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Set *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Set *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Set_to_tbox: unable to cast binary to Set *");
            }
			TBox * ret = set_to_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Set_to_tbox_floatset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Set *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Set *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Set_to_tbox: unable to cast binary to Set *");
            }
			TBox * ret = set_to_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Set_to_tbox_tstzset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Set *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Set *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Set_to_tbox: unable to cast binary to Set *");
            }
			TBox * ret = set_to_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Set_to_tbox_tstzset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Set *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Set *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Set_to_tbox: unable to cast binary to Set *");
            }
			TBox * ret = set_to_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Span_to_tbox_intspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Span_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = DataHelpers::span_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Span_to_tbox_intspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Span_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = DataHelpers::span_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Span_to_tbox_floatspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Span_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = DataHelpers::span_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Span_to_tbox_floatspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Span_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = DataHelpers::span_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Span_to_tbox_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Span_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = DataHelpers::span_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Span_to_tbox_tstzspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Span *s = nullptr;
            if (args0.GetSize() > 0) {
                s = (Span *)malloc(args0.GetSize());
                memcpy(s, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!s) {
                throw InternalException("Failure in Span_to_tbox: unable to cast binary to Span *");
            }
			TBox * ret = DataHelpers::span_tbox(s);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(s);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Spanset_to_tbox_intspanset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Datum d = DataHelpers::getDatum<string_t>(args0);
			TBox *ret = (TBox *)malloc(sizeof(TBox));		
			DataHelpers::spanset_tbox_slice(d, ret);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Spanset_to_tbox_intspanset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Datum d = DataHelpers::getDatum<string_t>(args0);
			TBox *ret = (TBox *)malloc(sizeof(TBox));		
			DataHelpers::spanset_tbox_slice(d, ret);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Spanset_to_tbox_floatspanset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Datum d = DataHelpers::getDatum<string_t>(args0);
			TBox *ret = (TBox *)malloc(sizeof(TBox));		
			DataHelpers::spanset_tbox_slice(d, ret);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Spanset_to_tbox_floatspanset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Datum d = DataHelpers::getDatum<string_t>(args0);
			TBox *ret = (TBox *)malloc(sizeof(TBox));		
			DataHelpers::spanset_tbox_slice(d, ret);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Spanset_to_tbox_tstzspanset(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            Datum d = DataHelpers::getDatum<string_t>(args0);
			TBox *ret = (TBox *)malloc(sizeof(TBox));		
			DataHelpers::spanset_tbox_slice(d, ret);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Spanset_to_tbox_tstzspanset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            Datum d = DataHelpers::getDatum<string_t>(args0);
			TBox *ret = (TBox *)malloc(sizeof(TBox));		
			DataHelpers::spanset_tbox_slice(d, ret);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Tbox_to_intspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_to_intspan: unable to cast binary to TBox *");
            }
			Span * ret = tbox_to_intspan(box);
            size_t ret_size = sizeof(Span);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Tbox_to_intspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_to_intspan: unable to cast binary to TBox *");
            }
			Span * ret = tbox_to_intspan(box);
            size_t ret_size = sizeof(Span);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Tbox_to_floatspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_to_floatspan: unable to cast binary to TBox *");
            }
			Span * ret = tbox_to_floatspan(box);
            size_t ret_size = sizeof(Span);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Tbox_to_floatspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_to_floatspan: unable to cast binary to TBox *");
            }
			Span * ret = tbox_to_floatspan(box);
            size_t ret_size = sizeof(Span);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Tbox_to_tstzspan(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_to_tstzspan: unable to cast binary to TBox *");
            }
			Span * ret = tbox_to_tstzspan(box);
            size_t ret_size = sizeof(Span);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

bool TboxFunctions::Tbox_to_tstzspan_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    bool success = true;
    try {
        UnaryExecutor::Execute<string_t, string_t>(
		source, result, count,
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_to_tstzspan: unable to cast binary to TBox *");
            }
			Span * ret = tbox_to_tstzspan(box);
            size_t ret_size = sizeof(Span);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    } catch (const std::exception &e) {
        throw InternalException(e.what());
        success = false;
    }
    return success;
}

void TboxFunctions::Tbox_hasx(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, bool>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_hasx: unable to cast binary to TBox *");
            }
			bool ret = tbox_hasx(box);
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_hast(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, bool>(
		args.data[0], result, args.size(),
		[&](string_t args0) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_hast: unable to cast binary to TBox *");
            }
			bool ret = tbox_hast(box);
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_xmin(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, double>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_xmin: unable to cast binary to TBox *");
            }
			double ret;		
			if (!tbox_xmin(box, &ret)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return double();		
			}
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_xmin_inc(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, bool>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_xmin_inc: unable to cast binary to TBox *");
            }
			bool ret;		
			if (!tbox_xmin_inc(box, &ret)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return bool();		
			}
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_xmax(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, double>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_xmax: unable to cast binary to TBox *");
            }
			double ret;		
			if (!tbox_xmax(box, &ret)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return double();		
			}
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_xmax_inc(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, bool>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_xmax_inc: unable to cast binary to TBox *");
            }
			bool ret;		
			if (!tbox_xmax_inc(box, &ret)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return bool();		
			}
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_tmin(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, timestamp_tz_t>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_tmin: unable to cast binary to TBox *");
            }
			TimestampTz ret_meos;		
			if (!tbox_tmin(box, &ret_meos)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return timestamp_tz_t();		
			}		
			timestamp_tz_t ret = MeosToDuckDBTimestamp((timestamp_tz_t)ret_meos);
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_tmin_inc(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, bool>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_tmin_inc: unable to cast binary to TBox *");
            }
			bool ret;		
			if (!tbox_tmin_inc(box, &ret)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return bool();		
			}
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_tmax(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, timestamp_tz_t>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_tmax: unable to cast binary to TBox *");
            }
			TimestampTz ret_meos;		
			if (!tbox_tmax(box, &ret_meos)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return timestamp_tz_t();		
			}		
			timestamp_tz_t ret = MeosToDuckDBTimestamp((timestamp_tz_t)ret_meos);
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_tmax_inc(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::ExecuteWithNulls<string_t, bool>(
		args.data[0], result, args.size(),
		[&](string_t args0, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_tmax_inc: unable to cast binary to TBox *");
            }
			bool ret;		
			if (!tbox_tmax_inc(box, &ret)) {		
				free(box);		
				mask.SetInvalid(idx);		
				return bool();		
			}
		
			free(box);
			return ret;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_shift_value_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_shift_value: unable to cast binary to TBox *");
            }
		
            Datum shift = DataHelpers::getDatum<int32_t>(args1);
			TBox * ret = tbox_shift_scale_value(box, shift, 0, true, false);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_shift_value_tbox_float(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_shift_value: unable to cast binary to TBox *");
            }
		
            Datum shift = DataHelpers::getDatum<int32_t>(args1);
			TBox * ret = tbox_shift_scale_value(box, shift, 0, true, false);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_shift_time(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, interval_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, interval_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_shift_time: unable to cast binary to TBox *");
            }
		
			MeosInterval *shift = IntervaltToInterval(args1);
			TBox * ret = tbox_shift_scale_time(box, shift, NULL);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(shift);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_scale_value_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_scale_value: unable to cast binary to TBox *");
            }
		
            Datum width = DataHelpers::getDatum<int32_t>(args1);
			TBox * ret = tbox_shift_scale_value(box, 0, width, false, true);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_scale_value_tbox_float(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_scale_value: unable to cast binary to TBox *");
            }
		
            Datum width = DataHelpers::getDatum<int32_t>(args1);
			TBox * ret = tbox_shift_scale_value(box, 0, width, false, true);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_scale_time(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, interval_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, interval_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_scale_time: unable to cast binary to TBox *");
            }
		
			MeosInterval *duration = IntervaltToInterval(args1);
			TBox * ret = tbox_shift_scale_time(box, NULL, duration);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(duration);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_shift_scale_value_tbox_integer_integer(DataChunk &args, ExpressionState &state, Vector &result) {
    TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
		args.data[0], args.data[1], args.data[2], result, args.size(),
		[&](string_t args0, int32_t args1, int32_t args2) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_shift_scale_value: unable to cast binary to TBox *");
            }
		
            Datum shift = DataHelpers::getDatum<int32_t>(args1);
		
            Datum width = DataHelpers::getDatum<int32_t>(args2);
			TBox * ret = tbox_shift_scale_value(box, shift, width, true, true);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_shift_scale_value_tbox_float_float(DataChunk &args, ExpressionState &state, Vector &result) {
    TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
		args.data[0], args.data[1], args.data[2], result, args.size(),
		[&](string_t args0, int32_t args1, int32_t args2) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_shift_scale_value: unable to cast binary to TBox *");
            }
		
            Datum shift = DataHelpers::getDatum<int32_t>(args1);
		
            Datum width = DataHelpers::getDatum<int32_t>(args2);
			TBox * ret = tbox_shift_scale_value(box, shift, width, true, true);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_shift_scale_time(DataChunk &args, ExpressionState &state, Vector &result) {
    TernaryExecutor::Execute<string_t, interval_t, interval_t, string_t>(
		args.data[0], args.data[1], args.data[2], result, args.size(),
		[&](string_t args0, interval_t args1, interval_t args2) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_shift_scale_time: unable to cast binary to TBox *");
            }
		
			MeosInterval *shift = IntervaltToInterval(args1);
		
			MeosInterval *duration = IntervaltToInterval(args2);
			TBox * ret = tbox_shift_scale_time(box, shift, duration);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(shift);		
			free(duration);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_expand_value_tbox_integer(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_expand_value: unable to cast binary to TBox *");
            }
		
            Datum value = DataHelpers::getDatum<int32_t>(args1);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args1);
			TBox *ret = tbox_expand_value(box, value, basetype);		
			if (!ret) {		
				free(box);		
				mask.SetInvalid(idx);		
				return string_t();		
			}
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_expand_value_tbox_float(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_expand_value: unable to cast binary to TBox *");
            }
		
            Datum value = DataHelpers::getDatum<int32_t>(args1);
			meosType basetype = DataHelpers::getMeosType<int32_t>(args1);
			TBox *ret = tbox_expand_value(box, value, basetype);		
			if (!ret) {		
				free(box);		
				mask.SetInvalid(idx);		
				return string_t();		
			}
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_expand_time(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::ExecuteWithNulls<string_t, interval_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, interval_t args1, ValidityMask &mask, idx_t idx) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_expand_time: unable to cast binary to TBox *");
            }
		
			MeosInterval *interval = IntervaltToInterval(args1);
			TBox *ret = tbox_expand_time(box, interval);		
			if (!ret) {		
				free(box);		
				mask.SetInvalid(idx);		
				return string_t();		
			}
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(interval);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

void TboxFunctions::Tbox_round(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, int32_t, string_t>(
		args.data[0], args.data[1], result, args.size(),
		[&](string_t args0, int32_t args1) {
            TBox *box = nullptr;
            if (args0.GetSize() > 0) {
                box = (TBox *)malloc(args0.GetSize());
                memcpy(box, args0.GetDataUnsafe(), args0.GetSize());
            }
			if (!box) {
                throw InternalException("Failure in Tbox_round: unable to cast binary to TBox *");
            }
		
			int32_t maxdd = args1;
			TBox * ret = tbox_round(box, maxdd);
            size_t ret_size = sizeof(TBox);
            char *ret_data = (char*)malloc(ret_size);
            memcpy(ret_data, ret, ret_size);
            string_t blob(ret_data, ret_size);
		
			free(box);		
			free(ret);
			return blob;
		}
	);
    if (args.size() == 1) {
        result.SetVectorType(VectorType::CONSTANT_VECTOR);
    }
}

} // namespace duckdb