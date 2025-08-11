#include "set.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "time_util.hpp"


extern "C" {     
    #include "meos.h"    
    #include "meos_internal.h"   
    #include "meos_geo.h"
}

namespace duckdb {

#define DEFINE_SET_TYPE(NAME)                                        \
    LogicalType SetTypes::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::BLOB);             \
        type.SetAlias(#NAME);                                        \
        return type;                                                 \
    }

DEFINE_SET_TYPE(intset)
DEFINE_SET_TYPE(bigintset)
DEFINE_SET_TYPE(floatset)
DEFINE_SET_TYPE(textset)
DEFINE_SET_TYPE(dateset)
DEFINE_SET_TYPE(tstzset)

#undef DEFINE_SET_TYPE

void SetTypes::RegisterTypes(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "intset", intset());
    ExtensionUtil::RegisterType(db, "bigintset", bigintset());
    ExtensionUtil::RegisterType(db, "floatset", floatset());
    ExtensionUtil::RegisterType(db, "textset", textset());
    ExtensionUtil::RegisterType(db, "dateset", dateset());
    ExtensionUtil::RegisterType(db, "tstzset", tstzset());    
}

const std::vector<LogicalType> &SetTypes::AllTypes() {
    static std::vector<LogicalType> types = {
        intset(),
        bigintset(),
        floatset(),
        textset(),
        dateset(),
        tstzset()
    };
    return types;
}

meosType SetTypeMapping::GetMeosTypeFromAlias(const std::string &alias) {
    static const std::unordered_map<std::string, meosType> alias_to_type = {
        {"intset", T_INTSET},
        {"bigintset", T_BIGINTSET},
        {"floatset", T_FLOATSET},
        {"textset", T_TEXTSET},
        {"dateset", T_DATESET},
        {"tstzset", T_TSTZSET}                
    };

    auto it = alias_to_type.find(alias);
    if (it != alias_to_type.end()) {
        return it->second;
    } else {
        return T_UNKNOWN;
    }
}

LogicalType SetTypeMapping::GetChildType(const LogicalType &type) {
    auto alias = type.ToString();
    if (alias == "intset") return LogicalType::INTEGER;
    if (alias == "bigintset") return LogicalType::BIGINT;
    if (alias == "floatset") return LogicalType::DOUBLE;
    if (alias == "textset") return LogicalType::VARCHAR;
    if (alias == "dateset") return LogicalType::DATE;
    if (alias == "tstzset") return LogicalType::TIMESTAMP_TZ;    
    throw NotImplementedException("GetChildType: unsupported alias: " + alias);
}


// Register all cast functions 
void SetTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    for (const auto &set_type : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterCastFunction(
            instance,
            set_type,                      
            LogicalType::VARCHAR,   
            SetFunctions::Set_to_text   
        ); // Blob to text
        ExtensionUtil::RegisterCastFunction(
            instance,
            LogicalType::VARCHAR, 
            set_type,                                    
            SetFunctions::Text_to_set   
        ); // text to blob
        
        auto base_type = SetTypeMapping::GetChildType(set_type);
        ExtensionUtil::RegisterCastFunction(
            instance,
            base_type,
            set_type,
            SetFunctions::Value_to_set_cast // set from base type
        );

        ExtensionUtil::RegisterCastFunction(
            instance,
            SetTypes::intset(),
            SetTypes::floatset(),
            SetFunctions::Intset_to_floatset_cast // intset -> floatset 
        );

        ExtensionUtil::RegisterCastFunction(
            instance,
            SetTypes::floatset(),
            SetTypes::intset(),
            SetFunctions::Floatset_to_intset_cast // floatset --> intset
        );
        
        ExtensionUtil::RegisterCastFunction(
            instance,
            SetTypes::dateset(),
            SetTypes::tstzset(),
            SetFunctions::Dateset_to_tstzset_cast // dateset -> tstzset
        );
        
        ExtensionUtil::RegisterCastFunction(
            instance,
            SetTypes::tstzset(),
            SetTypes::dateset(),
            SetFunctions::Tstzset_to_dateset_cast // tstz -> dateset 
        );

    }
}

void SetTypes::RegisterScalarFunctions(DatabaseInstance &db) {    
    for (const auto &set_type : SetTypes::AllTypes()) {
        auto base_type = SetTypeMapping::GetChildType(set_type);         

        // Register: asText
        if (set_type == SetTypes::floatset()) {            
            ExtensionUtil::RegisterFunction( // asText(floatset)
                db, ScalarFunction("asText", {set_type}, LogicalType::VARCHAR, SetFunctions::Set_as_text)
            );
            
            ExtensionUtil::RegisterFunction( // asText(floatset, int)
                db, ScalarFunction("asText", {set_type, LogicalType::INTEGER}, LogicalType::VARCHAR, SetFunctions::Set_as_text)
            );
        } else {            
            ExtensionUtil::RegisterFunction( // All other set types
                db, ScalarFunction("asText", {set_type}, LogicalType::VARCHAR, SetFunctions::Set_as_text)
            );
        }

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("set", {base_type}, set_type, SetFunctions::Value_to_set)                 
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("intset", {SetTypes::floatset()}, SetTypes::intset(), SetFunctions::Floatset_to_intset)                 
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("floatset", {SetTypes::intset()}, SetTypes::floatset(), SetFunctions::Intset_to_floatset)                 
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("dateset", {SetTypes::tstzset()}, SetTypes::dateset(), SetFunctions::Tstzset_to_dateset)                 
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("tstzset", {SetTypes::dateset()}, SetTypes::tstzset(), SetFunctions::Dateset_to_tstzset)                 
        );

        ExtensionUtil::RegisterFunction(
            db, 
            ScalarFunction("memSize",{set_type}, LogicalType::INTEGER, SetFunctions::Set_mem_size)
        );
        
        ExtensionUtil::RegisterFunction(
            db, 
            ScalarFunction("numValues", {set_type}, LogicalType::INTEGER,SetFunctions::Set_num_values)
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("startValue", {set_type}, base_type, SetFunctions::Set_start_value)
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("endValue", {set_type}, base_type, SetFunctions::Set_end_value)
        );

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("valueN", {set_type, LogicalType::INTEGER}, base_type, SetFunctions::Set_value_n)
        );

        ExtensionUtil::RegisterFunction(
            db, ScalarFunction("getValues", {set_type}, LogicalType::LIST(base_type), SetFunctions::Set_values)
        );

        if (set_type == SetTypes::intset() || set_type == SetTypes::bigintset() 
            ||set_type == SetTypes::floatset() || set_type == SetTypes::dateset()){

            ExtensionUtil::RegisterFunction(
                db, ScalarFunction("shift", {set_type, LogicalType::INTEGER}, set_type, SetFunctions::Numset_shift)
            ); 
        }
    }
}

// --- AsText ---
void SetFunctions::Set_as_text(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vec = args.data[0];
    input_vec.Flatten(args.size());

    bool has_digits = args.ColumnCount() > 1;
    Vector *digits_vec_ptr = has_digits ? &args.data[1] : nullptr;
    if (has_digits) digits_vec_ptr->Flatten(args.size());

    for (idx_t i = 0; i < args.size(); i++) {
        if (FlatVector::IsNull(input_vec, i) || (has_digits && FlatVector::IsNull(*digits_vec_ptr, i))) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        auto blob = FlatVector::GetData<string_t>(input_vec)[i];
        int digits = has_digits ? FlatVector::GetData<int32_t>(*digits_vec_ptr)[i] : 15;

        const uint8_t *data = (const uint8_t *)blob.GetData();
        size_t size = blob.GetSize();

        Set *s = (Set *)malloc(size);
        memcpy(s, data, size);

        char *cstr = set_out(s, digits);
        auto str = StringVector::AddString(result, cstr);
        FlatVector::GetData<string_t>(result)[i] = str;

        free(s);
        free(cstr);
    }
}


// --- Cast From String ---
bool SetFunctions::Set_to_text(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    auto result_data = FlatVector::GetData<string_t>(result); 

    for (idx_t i = 0; i < count; ++i) {
        if (FlatVector::IsNull(source, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        Value val = source.GetValue(i);
        const string_t &blob = StringValue::Get(val);
        const uint8_t *data = (const uint8_t *)(blob.GetData());
        size_t size = blob.GetSize();

        Set *s = (Set*)malloc(size);                

        memcpy(s, data, size);             
        char *cstr = set_out(s, 15);  
        result_data[i] = StringVector::AddString(result, cstr);

        free(cstr);
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}

bool SetFunctions::Text_to_set(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);
    result.SetVectorType(VectorType::FLAT_VECTOR);

    auto target_type = result.GetType();
    meosType set_type = SetTypeMapping::GetMeosTypeFromAlias(target_type.GetAlias());

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input) -> string_t {
            std::string input_str(input.GetDataUnsafe(), input.GetSize());
            Set *s = nullptr;

            if (set_type == T_TEXTSET && !input_str.empty() && input_str.front() != '{') {
                // Treat as a single text element for TEXTSET
                text *txt = (text *)malloc(VARHDRSZ + input_str.size());
                SET_VARSIZE(txt, VARHDRSZ + input_str.size());
                memcpy(VARDATA(txt), input_str.c_str(), input_str.size());

                s = value_set(PointerGetDatum(txt), T_TEXT);                
            } else {
                s = set_in(input_str.c_str(), set_type);                              
            }            

            string_t blob = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
            free(s);
            return blob;
        }
    );

    return true;
}

// --- Set constructor from list ---
void SetFunctions::Set_constructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &list_input = args.data[0];
    auto meos_type = SetTypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());

    UnaryExecutor::Execute<list_entry_t, string_t>(
        list_input, result, args.size(),
        [&](list_entry_t list_entry) -> string_t {
            auto &child = ListVector::GetEntry(list_input);
            child.Flatten(args.size());  

            idx_t offset = list_entry.offset;
            idx_t length = list_entry.length;

            Datum *values = (Datum *)malloc(sizeof(Datum) * length);            

            for (idx_t i = 0; i < length; ++i) {
                idx_t idx = offset + i;

                switch (meos_type) {
                    case T_INTSET: {
                        int32_t v = FlatVector::GetData<int32_t>(child)[idx];
                        values[i] = Datum(v);
                        break;
                    }
                    case T_BIGINTSET: {
                        int64_t v = FlatVector::GetData<int64_t>(child)[idx];
                        values[i] = Datum(v);
                        break;
                    }
                    case T_FLOATSET: {
                        double v = FlatVector::GetData<double>(child)[idx];
                        Datum d;
                        static_assert(sizeof(Datum) == sizeof(double));
                        memcpy(&d, &v, sizeof(double));
                        values[i] = d;
                        break;
                    }
                    case T_TEXTSET: {
                        string_t str = FlatVector::GetData<string_t>(child)[idx];
                        size_t len = str.GetSize();
                        const char *cstr = str.GetData();

                        text *txt = (text *)malloc(VARHDRSZ + len);
                        SET_VARSIZE(txt, VARHDRSZ + len);
                        memcpy(VARDATA(txt), cstr, len);

                        values[i] = (Datum)txt;                        
                        break;
                    }
                    case T_DATESET: {
                        date_t d = FlatVector::GetData<date_t>(child)[idx];
                        values[i] = Datum((int32_t)ToMeosDate(d));
                        break;
                    }
                    case T_TSTZSET: {
                        timestamp_t ts = FlatVector::GetData<timestamp_t>(child)[idx];
                        values[i] = Datum(ToMeosTimestamp(ts));
                        break;
                    }
                    default:
                        free(values);
                        throw InvalidInputException("Unsupported type in SetFromList");
                }
            }

            meosType base_type = settype_basetype(meos_type);
            Set *s = set_make_free(values, (int)length, base_type, true);
                        
            size_t size = VARSIZE(s);            
            string_t blob = StringVector::AddStringOrBlob(result, (const char*)s, size);
            
            free(s);            
            return blob;
        }
    );
}

// Conversion function: base type -> set 
static inline void Write_set(Vector &result, idx_t row, Set *s) {
    auto out = FlatVector::GetData<string_t>(result);
    out[row] = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
    free(s);
}

static inline void Value_to_set_core(Vector &source, Vector &result, idx_t count, meosType base_type) {
    source.Flatten(count);
    result.SetVectorType(VectorType::FLAT_VECTOR);
    
    auto handle_null = [&](idx_t row) {
        FlatVector::SetNull(result, row, true);
    };

    switch (base_type) {
        case T_INT4: {
            auto in = FlatVector::GetData<int32_t>(source);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) { handle_null(i); continue; }
                Datum d = Datum(in[i]);               
                Set *s = value_set(d, T_INT4);
                Write_set(result, i, s);
            }
            break;
        }
        case T_INT8: {
            auto in = FlatVector::GetData<int64_t>(source);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) { handle_null(i); continue; }
                Datum d = Datum(in[i]);               
                Set *s = value_set(d, T_INT8);
                Write_set(result, i, s);
            }
            break;
        }
        case T_FLOAT8: {
            auto in = FlatVector::GetData<double>(source);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) { handle_null(i); continue; }
                Datum d = Float8GetDatum(in[i]);
                Set *s = value_set(d, T_FLOAT8);
                Write_set(result, i, s);
            }
            break;
        }
        case T_TEXT: {
            auto in = FlatVector::GetData<string_t>(source);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) {
                    handle_null(i);
                    continue;
                }                
                const char *data_ptr = in[i].GetDataUnsafe();
                size_t len = in[i].GetSize();                
                text *txt = (text *)malloc(VARHDRSZ + len);
                SET_VARSIZE(txt, VARHDRSZ + len);
                memcpy(VARDATA(txt), data_ptr, len);
                Set *s = value_set(PointerGetDatum(txt), T_TEXT);
                Write_set(result, i, s);
                free(txt); 
            }
            break;
        }
            
        case T_DATE: {
            auto in = FlatVector::GetData<date_t>(source);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) { handle_null(i); continue; }
                int32_t days = (int32_t)ToMeosDate(in[i]);
                Datum d = Datum(days);
                Set *s = value_set(d, T_DATE);
                Write_set(result, i, s);
            }
            break;
        }
        case T_TIMESTAMPTZ: {
            auto in = FlatVector::GetData<timestamp_t>(source);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) { handle_null(i); continue; }
                auto meos_ts = ToMeosTimestamp(in[i]);        
                Datum d = Datum(meos_ts);
                Set *s = value_set(d, T_TIMESTAMPTZ);
                Write_set(result, i, s);
            }
            break;
        }
        default:
            throw NotImplementedException("SetConversion: unsupported base type for conversion to set");
    }
}

// --- CAST (conversion: base -> set) ----

bool SetFunctions::Value_to_set_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    auto target_type = result.GetType();
    meosType set_type  = SetTypeMapping::GetMeosTypeFromAlias(target_type.GetAlias());
    meosType base_type = settype_basetype(set_type);

    Value_to_set_core(source, result, count, base_type);
    return true;
}

// --- SCALAR function (base -> set) ----
void SetFunctions::Value_to_set(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &source = args.data[0];
    auto out_type = result.GetType();
    meosType set_type  = SetTypeMapping::GetMeosTypeFromAlias(out_type.GetAlias());
    meosType base_type = settype_basetype(set_type);

    Value_to_set_core(source, result, args.size(), base_type);
}

// --- Conversion: intset <-> floatset ---
static void Intset_to_floatset_common(Vector &source, Vector &result, idx_t count) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t blob) -> string_t {
            const Set *src_set = (const Set *)blob.GetDataUnsafe();
            VALIDATE_INTSET(src_set, NULL);

            Datum *values = (Datum *)malloc(sizeof(Datum) * src_set->count);
            for (int j = 0; j < src_set->count; j++) {
                int32_t v_src = Datum(SET_VAL_N(src_set, j));
                values[j] = Float8GetDatum((double)v_src);
            }

            Set *dst_set = set_make_free(values, src_set->count, T_FLOAT8, true);
            string_t out = StringVector::AddStringOrBlob(result, (const char *)dst_set, VARSIZE(dst_set));
            free(dst_set);
            return out;
        }
    );
}

static void Floatset_to_intset_common(Vector &source, Vector &result, idx_t count) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t blob) -> string_t {
            const Set *src_set = (const Set *)blob.GetDataUnsafe();
            VALIDATE_FLOATSET(src_set, NULL);

            Datum *values = (Datum *)malloc(sizeof(Datum) * src_set->count);
            for (int j = 0; j < src_set->count; j++) {
                double v_src = DatumGetFloat8(SET_VAL_N(src_set, j));
                values[j] = Datum((int32_t)v_src);
            }

            Set *dst_set = set_make_free(values, src_set->count, T_INT4, true);
            string_t out = StringVector::AddStringOrBlob(result, (const char *)dst_set, VARSIZE(dst_set));
            free(dst_set);
            return out;
        }
    );
}


void SetFunctions::Intset_to_floatset(DataChunk &args, ExpressionState &state, Vector &result) {
    Intset_to_floatset_common(args.data[0], result, args.size());
}

void SetFunctions::Floatset_to_intset(DataChunk &args, ExpressionState &state, Vector &result) {
    Floatset_to_intset_common(args.data[0], result, args.size());
}

bool SetFunctions::Intset_to_floatset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    Intset_to_floatset_common(source, result, count);
    return true;    
}

bool SetFunctions::Floatset_to_intset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    Floatset_to_intset_common(source, result, count);
    return true;
}

// --- Conversion: tstzset <-> dateset ---

// dateset -> tstzset
static void Dateset_to_tstzset_common(Vector &source, Vector &result, idx_t count) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t blob) -> string_t {
            const Set *src = (const Set *)blob.GetDataUnsafe();
            VALIDATE_DATESET(src, nullptr);

            Datum *vals = (Datum *)malloc(sizeof(Datum) * src->count);
            for (int i = 0; i < src->count; i++) {
                DateADT d = (DateADT)(SET_VAL_N(src, i));
                TimestampTz ts = date_to_timestamptz(d);
                vals[i] = int64(ts);
            }
            // All distinct dates -> distinct timestamptz
            Set *dst = set_make_free(vals, src->count, T_TIMESTAMPTZ, false);
            string_t out = StringVector::AddStringOrBlob(result, (const char *)dst, VARSIZE(dst));
            free(dst);
            return out;
        }
    );
}

// tstzset -> dateset
static void Tstzset_to_dateset_common(Vector &source, Vector &result, idx_t count) {
    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t blob) -> string_t {
            const Set *src = (const Set *)blob.GetDataUnsafe();
            VALIDATE_TSTZSET(src, nullptr);

            Datum *vals = (Datum *)malloc(sizeof(Datum) * src->count);
            for (int i = 0; i < src->count; i++) {
                TimestampTz ts = (TimestampTz)(SET_VAL_N(src, i));
                DateADT d = timestamptz_to_date(ts);
                vals[i] = int32(d);
            }            
            Set *dst = set_make_free(vals, src->count, T_DATE, true);
            string_t out = StringVector::AddStringOrBlob(result, (const char *)dst, VARSIZE(dst));
            free(dst);
            return out;
        }
    );
}

// --- SCALAR: dateset -> tstzset ---
void SetFunctions::Dateset_to_tstzset(DataChunk &args, ExpressionState &state, Vector &result) {
    Dateset_to_tstzset_common(args.data[0], result, args.size());
}

// --- SCALAR: tstzset -> dateset ---
void SetFunctions::Tstzset_to_dateset(DataChunk &args, ExpressionState &state, Vector &result) {
    Tstzset_to_dateset_common(args.data[0], result, args.size());
}

// --- CAST: dateset -> tstzset ---
bool SetFunctions::Dateset_to_tstzset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    Dateset_to_tstzset_common(source, result, count);
    return true;
}

// --- CAST: tstzset -> dateset ---
bool SetFunctions::Tstzset_to_dateset_cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    Tstzset_to_dateset_common(source, result, count);
    return true;
}

// --- memSize ---
void SetFunctions::Set_mem_size(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> int32_t {                                 
            const uint8_t *data = (const uint8_t *)input_blob.GetData();
            size_t size = input_blob.GetSize();
            Set *s = (Set*)malloc(size);
            memcpy(s, data, size);            
            int mem_size = set_mem_size(s);  
            free(s);
            return mem_size;
        });
}


// --- numValue ---
void SetFunctions::Set_num_values(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> int32_t {
            const uint8_t *data = (const uint8_t *)input_blob.GetData();
            size_t size = input_blob.GetSize();
            Set *s = (Set*)malloc(size);
            memcpy(s, data, size);
            int count = set_num_values(s);
            free(s);
            return count;
        });
}

// --- startValue ---
void SetFunctions::Set_start_value(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<string_t, int32_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int32_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);
                    Datum d = set_start_value(s);
                    free(s);
                    return int32(d);
                });
            break;

        case T_INT8:
            UnaryExecutor::Execute<string_t, int64_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int64_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);
                    Datum d = set_start_value(s);
                    free(s);
                    return int64(d);
                });
            break;

        case T_FLOAT8:
            UnaryExecutor::Execute<string_t, double>(
                input, result, args.size(),
                [&](string_t input_blob) -> double {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_start_value(s);
                    free(s);
                    return DatumGetFloat8(d);
                });
            break;

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> string_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_start_value(s);
                    free(s);                                        
                    text *txt = (text *)DatumGetPointer(d);
                    int len = VARSIZE(txt) - VARHDRSZ;
                    string str(VARDATA(txt), len);
                    return StringVector::AddString(result, str); 
                });
            break;

        case T_DATE:
            UnaryExecutor::Execute<string_t, date_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> date_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_start_value(s);
                    free(s);
                    return date_t(int32(FromMeosDate(d)));
                });
            break;

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<string_t, timestamp_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> timestamp_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_start_value(s);
                    free(s);
                    return timestamp_t(int64(FromMeosTimestamp(d)));
                });
            break;

        default:
            throw NotImplementedException("startValue: Unsupported set base type.");
    }
}


// --- endValue ---
void SetFunctions::Set_end_value(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<string_t, int32_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int32_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_end_value(s);
                    free(s);
                    return int32(d);
                });
            break;

        case T_INT8:
            UnaryExecutor::Execute<string_t, int64_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int64_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_end_value(s);
                    free(s);
                    return int64(d);
                });
            break;

        case T_FLOAT8:
            UnaryExecutor::Execute<string_t, double>(
                input, result, args.size(),
                [&](string_t input_blob) -> double {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_end_value(s);
                    free(s);
                    return DatumGetFloat8(d);
                });
            break;

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> string_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_end_value(s);
                    free(s);                                        
                    text *txt = (text *)DatumGetPointer(d);
                    int len = VARSIZE(txt) - VARHDRSZ;
                    string str(VARDATA(txt), len);
                    return StringVector::AddString(result, str); 
                });
            break;

        case T_DATE:
            UnaryExecutor::Execute<string_t, date_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> date_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_end_value(s);
                    free(s);
                    return date_t(int32(FromMeosDate(d)));
                });
            break;

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<string_t, timestamp_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> timestamp_t {
                    const uint8_t *data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = (Set*)malloc(size);
                    memcpy(s, data, size);                    
                    Datum d = set_end_value(s);
                    free(s);
                    return timestamp_t(int64(FromMeosTimestamp(d)));
                });
            break;

        default:
            throw NotImplementedException("startValue: Unsupported set base type.");
    }
}

// --- valueN ---
void SetFunctions::Set_value_n(DataChunk &args, ExpressionState &state, Vector &result_vec) {
    auto &set_vec = args.data[0];
    auto &index_vec = args.data[1];

    const auto set_type = SetTypeMapping::GetMeosTypeFromAlias(set_vec.GetType().ToString());
    const auto base_type = settype_basetype(set_type);

    result_vec.SetVectorType(VectorType::FLAT_VECTOR);
    auto &validity = FlatVector::Validity(result_vec);

    for (idx_t i = 0; i < args.size(); i++) {
        validity.SetInvalid(i);

        if (set_vec.GetValue(i).IsNull() || index_vec.GetValue(i).IsNull())
            continue;
        
        int32_t index = FlatVector::GetData<int32_t>(index_vec)[i];
        auto blob = FlatVector::GetData<string_t>(set_vec)[i];
        const uint8_t *data = (const uint8_t *)blob.GetData();
        size_t size = blob.GetSize();        
        Set *s = (Set*)malloc(size);
        memcpy(s, data, size);

        if (!s) continue;

        Datum d;
        bool found = set_value_n(s, index, &d);
        free(s);

        if (!found) continue;
        
        switch (base_type) {
            case T_INT4:
                FlatVector::GetData<int32_t>(result_vec)[i] = int32(d);
                break;
            case T_INT8:
                FlatVector::GetData<int64_t>(result_vec)[i] = int64(d);
                break;
            case T_FLOAT8:
                FlatVector::GetData<double>(result_vec)[i] = DatumGetFloat8(d);
                break;
            case T_TEXT: {
                text *txt = (text *)DatumGetPointer(d);
                int len = VARSIZE(txt) - VARHDRSZ;
                string str(VARDATA(txt), len);
                FlatVector::GetData<string_t>(result_vec)[i] = StringVector::AddString(result_vec, str);
                break;
            }
            case T_DATE: {
                int32_t raw = int32(d);
                FlatVector::GetData<date_t>(result_vec)[i] = date_t(FromMeosDate(raw));
                break;
            }
            case T_TIMESTAMPTZ: {
                int64_t raw = int64(d);
                FlatVector::GetData<timestamp_t>(result_vec)[i] = timestamp_t(FromMeosTimestamp(raw));
                break;
            }
            default:
                throw NotImplementedException("valueN: unsupported set base type");
        }

        validity.SetValid(i);
    }
}

// --- getValues ---

void SetFunctions::Set_values(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    idx_t row_count = args.size();
    
    auto set_type_alias = SetTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type_alias); // MEOS base type enum
    auto child_type = SetTypeMapping::GetChildType(input.GetType()); // DuckDB LogicalType
    
    auto &result_validity = FlatVector::Validity(result);
    auto list_entries = FlatVector::GetData<list_entry_t>(result);
    auto &child_vector = ListVector::GetEntry(result);
    
    child_vector.SetVectorType(VectorType::FLAT_VECTOR);
    ListVector::Reserve(result, row_count); 
    idx_t total_offset = 0;

    for (idx_t i = 0; i < row_count; ++i) {
        if (input.GetValue(i).IsNull()) {
            result_validity.SetInvalid(i);
            continue;
        }

        string_t blob = FlatVector::GetData<string_t>(input)[i];
        const uint8_t *data = (const uint8_t *)(blob.GetData());
        size_t size = blob.GetSize();
        Set *s = (Set*)malloc(size);
        memcpy(s, data, size);
        if (!s) {
            result_validity.SetInvalid(i);
            continue;
        }

        uint64_t count = s->count;
        Datum *values = set_vals(s);
        
        ListVector::SetListSize(result, total_offset + count);
        list_entries[i] = list_entry_t{total_offset, count};
        
        switch (base_type) {
            case T_INT4: {
                auto data = FlatVector::GetData<int32_t>(child_vector);
                for (int j = 0; j < count; ++j) {
                    data[total_offset + j] = int32(values[j]);
                }
                break;
            }
            case T_INT8: {
                auto data = FlatVector::GetData<int64_t>(child_vector);
                for (int j = 0; j < count; ++j) {
                    data[total_offset + j] = int64(values[j]);
                }
                break;
            }
            case T_FLOAT8: {
                auto data = FlatVector::GetData<double>(child_vector);
                for (int j = 0; j < count; ++j) {
                    data[total_offset + j] = DatumGetFloat8(values[j]);
                }
                break;
            }
            case T_TEXT: {
                auto data = FlatVector::GetData<string_t>(child_vector);
                for (int j = 0; j < count; ++j) {
                    text *txt = (text *)DatumGetPointer(values[j]);
                    string str(VARDATA(txt), VARSIZE(txt) - VARHDRSZ);
                    data[total_offset + j] = StringVector::AddString(child_vector, str);
                }
                break;
            }
            case T_DATE: {
                auto data = FlatVector::GetData<date_t>(child_vector);
                for (int j = 0; j < count; ++j) {
                    data[total_offset + j] = date_t(FromMeosDate(int32(values[j])));
                }
                break;
            }
            case T_TIMESTAMPTZ: {
                auto data = FlatVector::GetData<timestamp_t>(child_vector);
                for (int j = 0; j < count; ++j) {
                    data[total_offset + j] = timestamp_t(FromMeosTimestamp(int64(values[j])));
                }
                break;
            }
            default:
                free(values);
                free(s);
                throw NotImplementedException("Unsupported base type in getValues");
        }

        total_offset += count;
        result_validity.SetValid(i);
        free(values);
        free(s);
    }
}

// --- shift ---
static inline string_t Numset_shift_common( const string_t &blob, Datum shift_datum,
                                        meosType validate_set_type,Vector &result) {
    const Set *s = (const Set *)blob.GetDataUnsafe();
    switch(validate_set_type){
        case T_INTSET: VALIDATE_INTSET(s, NULL); break;
        case T_FLOATSET: VALIDATE_FLOATSET(s, NULL); break;
        case T_BIGINTSET: VALIDATE_BIGINTSET(s, NULL); break;
        case T_DATESET: VALIDATE_DATESET(s,NULL); break; 
        default: break; 

    }    
    Set *r = numset_shift_scale(s, shift_datum, 0, /*do_shift=*/true, /*do_scale=*/false);
    string_t out = StringVector::AddStringOrBlob(result, (const char *)r, VARSIZE(r));
    free(r);
    return out;
}

void SetFunctions::Numset_shift(DataChunk &args, ExpressionState &state, Vector &result) {    
    auto &set_vec  = args.data[0];
    auto out_type  = result.GetType();    
    meosType set_type  = SetTypeMapping::GetMeosTypeFromAlias(out_type.GetAlias());    

    switch (set_type) {
        case T_INTSET: { // shift(intset, integer) -> intset
            BinaryExecutor::Execute<string_t, int32_t, string_t>(
                set_vec, args.data[1], result, args.size(),
                [&](string_t blob, int32_t shift) -> string_t {
                    return Numset_shift_common(blob, Datum(shift), set_type, result);
                });
            break;
        }
        case T_BIGINTSET: { // shift(bigintset, integer) -> bigintset
            BinaryExecutor::Execute<string_t, int32_t, string_t>(
                set_vec, args.data[1], result, args.size(),
                [&](string_t blob, int64_t shift) -> string_t {
                    return Numset_shift_common(blob, Datum(shift), set_type, result);
                });
            break;
        }
        case T_FLOATSET: { // shift(floatset, integer) -> floatset
            BinaryExecutor::Execute<string_t, int32_t, string_t>(
                set_vec, args.data[1], result, args.size(),
                [&](string_t blob, double shift) -> string_t {                    
                    return Numset_shift_common(blob, Float8GetDatum(shift), set_type, result);
                });
            break;
        }
        case T_DATESET: { // shift(dateset, integer) -> dateset
            BinaryExecutor::Execute<string_t, int32_t, string_t>(
                set_vec, args.data[1], result, args.size(),
                [&](string_t blob, int32_t shift_days) -> string_t {
                    return Numset_shift_common(blob, Datum(shift_days), set_type, result);
                });
            break;
        }
        default:
            throw NotImplementedException("shift(<set>): unsupported base type");
    }
}

// --- Unnest ---
struct SetUnnestBindData : public TableFunctionData {
    string_t blob;
    meosType set_type;
    LogicalType return_type;

    SetUnnestBindData(string_t blob, meosType set_type, LogicalType return_type)
        : blob(std::move(blob)), set_type(set_type), return_type(std::move(return_type)) {}
};


struct SetUnnestGlobalState : public GlobalTableFunctionState {
    idx_t idx = 0;
    std::vector<Value> values;    
};

static unique_ptr<FunctionData> SetUnnestBind(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {
    if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
        throw BinderException("SetUnnest: expects a non-null blob input");
    }

    auto in_val = input.inputs[0];
    if (in_val.type().id() != LogicalTypeId::BLOB) {
        throw BinderException("SetUnnest: expected BLOB as input");
    }

    string_t blob = StringValue::Get(in_val);

    auto duck_type = SetTypeMapping::GetChildType(in_val.type());
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(in_val.type().ToString());

    return_types.emplace_back(duck_type);
    names.emplace_back("unnest");

    return make_uniq<SetUnnestBindData>(blob, set_type, duck_type);
}

static unique_ptr<GlobalTableFunctionState> SetUnnestInit(ClientContext &context,
                                                          TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<SetUnnestBindData>();
    auto &blob = bind.blob;

    const uint8_t *data = (const uint8_t *)blob.GetData();
    size_t size = blob.GetSize();

    Set *s = (Set *)malloc(size);
    memcpy(s, data, size);

    auto state = make_uniq<SetUnnestGlobalState>();
    int count = s->count;

    for (int i = 1; i <= count; ++i) {
        Datum d;
        bool found = set_value_n(s, i, &d);
        if (!found) continue;

        switch (settype_basetype(bind.set_type)) {
            case T_INT4:
                state->values.emplace_back(Value::INTEGER((int32_t)d));
                break;
            case T_INT8:
                state->values.emplace_back(Value::BIGINT((int64_t)d));
                break;
            case T_FLOAT8:
                state->values.emplace_back(Value::DOUBLE(DatumGetFloat8(d)));
                break;
            case T_TEXT: {
                text *txt = (text *)DatumGetPointer(d);
                int len = VARSIZE(txt) - VARHDRSZ;
                std::string str(VARDATA(txt), len);
                state->values.emplace_back(Value(str));
                break;
            }
            case T_DATE:
                state->values.emplace_back(Value::DATE(date_t(FromMeosDate((int32_t)d))));
                break;
            case T_TIMESTAMPTZ:
                state->values.emplace_back(Value::TIMESTAMPTZ(timestamp_tz_t(FromMeosTimestamp((int64_t)d))));
                break;
            default:
                free(s);
                throw NotImplementedException("SetUnnest: unsupported base type");
        }
    }

    free(s);
    return std::move(state);
}

static void SetUnnestExec(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &state = input.global_state->Cast<SetUnnestGlobalState>();
    auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.values.size() - state.idx);

    for (idx_t i = 0; i < count; i++) {
        output.SetValue(0, i, state.values[state.idx++]);
    }

    output.SetCardinality(count);
}

void SetTypes::RegisterSetUnnest(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {
        TableFunction fn("SetUnnest",
                         {set_type},
                         SetUnnestExec,
                         SetUnnestBind,
                         SetUnnestInit);
        ExtensionUtil::RegisterFunction(db, fn);
    }
}

} // namespace duckdb
