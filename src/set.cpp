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

static inline Datum
Float8GetDatum(double X)
{
  union
  {
    double    value;
    int64    retval;
  }      myunion;

  myunion.value = X;
  return Datum(myunion.retval);
}

static inline double
DatumGetFloat8(Datum X)
{
  union
  {
    int64    value;
    double    retval;
  }      myunion;

  myunion.value = int64(X);
  return myunion.retval;
}

#define CStringGetDatum(X) PointerGetDatum(X)
#define PointerGetDatum(X) ((Datum) (X))    
#define SET_VARSIZE(PTR, len)        SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_4B(PTR,len) \
  (((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define VARSIZE(PTR)            VARSIZE_4B(PTR)
#define VARSIZE_4B(PTR) \
  ((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARDATA(PTR)            VARDATA_4B(PTR)
#define VARDATA_4B(PTR)    (((varattrib_4b *) (PTR))->va_4byte.va_data) 
#define FLEXIBLE_ARRAY_MEMBER	/* empty */
typedef union
{
  struct            /* Normal varlena (4-byte length) */
  {
    uint32    va_header;
    char    va_data[FLEXIBLE_ARRAY_MEMBER];
  }      va_4byte;
  struct            /* Compressed-in-line format */
  {
    uint32    va_header;
    uint32    va_tcinfo;  /* Original data size (excludes header) and
                 * compression method; see va_extinfo */
    char    va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
  }      va_compressed;
} varattrib_4b;

#define VARHDRSZ		((int32) sizeof(int32))

#define DEFINE_SET_TYPE(NAME)                                        \
    LogicalType SetTypes::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::BLOB);             \
        type.SetAlias(#NAME);                                        \
        return type;                                                 \
    }

DEFINE_SET_TYPE(INTSET)
DEFINE_SET_TYPE(BIGINTSET)
DEFINE_SET_TYPE(FLOATSET)
DEFINE_SET_TYPE(TEXTSET)
DEFINE_SET_TYPE(DATESET)
DEFINE_SET_TYPE(TSTZSET)

#undef DEFINE_SET_TYPE

void SetTypes::RegisterTypes(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "INTSET", INTSET());
    ExtensionUtil::RegisterType(db, "BIGINTSET", BIGINTSET());
    ExtensionUtil::RegisterType(db, "FLOATSET", FLOATSET());
    ExtensionUtil::RegisterType(db, "TEXTSET", TEXTSET());
    ExtensionUtil::RegisterType(db, "DATESET", DATESET());
    ExtensionUtil::RegisterType(db, "TSTZSET", TSTZSET());    
}

const std::vector<LogicalType> &SetTypes::AllTypes() {
    static std::vector<LogicalType> types = {
        INTSET(),
        BIGINTSET(),
        FLOATSET(),
        TEXTSET(),
        DATESET(),
        TSTZSET()
    };
    return types;
}

meosType SetTypeMapping::GetMeosTypeFromAlias(const std::string &alias) {
    static const std::unordered_map<std::string, meosType> alias_to_type = {
        {"INTSET", T_INTSET},
        {"BIGINTSET", T_BIGINTSET},
        {"FLOATSET", T_FLOATSET},
        {"TEXTSET", T_TEXTSET},
        {"DATESET", T_DATESET},
        {"TSTZSET", T_TSTZSET}                
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
    if (alias == "INTSET") return LogicalType::INTEGER;
    if (alias == "BIGINTSET") return LogicalType::BIGINT;
    if (alias == "FLOATSET") return LogicalType::DOUBLE;
    if (alias == "TEXTSET") return LogicalType::VARCHAR;
    if (alias == "DATESET") return LogicalType::DATE;
    if (alias == "TSTZSET") return LogicalType::TIMESTAMP_TZ;    
    throw NotImplementedException("GetChildType: unsupported alias: " + alias);
}


static void SetFromText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());

    UnaryExecutor::Execute<string_t, string_t>(
    input, result, args.size(),
    [&](string_t str) -> string_t {        
        Set *s = set_in(str.GetString().c_str(), set_type);
        size_t size;
        uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
        string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);

        free(wkb); free(s);
        return blob;         
    }

    );
}

void SetTypes::RegisterSet(DatabaseInstance &db) {    
    for (const auto &t : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction(t.ToString(), {LogicalType::VARCHAR}, t, SetFromText)
        );
    }
}

//AsText
static void AsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t blob_str) -> string_t {            
            const uint8_t *data = (const uint8_t *)blob_str.GetData();
            size_t size = blob_str.GetSize();            
            Set *s = set_from_wkb(data, size);
            if (!s) {
                throw InvalidInputException("Failed to parse Set from WKB blob");
            }
            
            char *cstr = set_out(s, 15); 
            auto result_str = StringVector::AddString(result, cstr);                       
            free(s);
            free(cstr);

            return result_str;
        }
    );
}

void SetTypes::RegisterSetAsText(DatabaseInstance &db) {    
    for (const auto &t : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction("asText", {t}, LogicalType::VARCHAR, AsText)
        );
    }

}

// Cast
bool SetBlobToText(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
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

        Set *s = set_from_wkb(data, size);
        if (!s) {
            throw InvalidInputException("Failed to decode Set from WKB");
        }

        char *cstr = set_out(s, 15);  
        result_data[i] = StringVector::AddString(result, cstr);

        free(cstr);
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}

bool TextToSet(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
    source.Flatten(count);

    auto target_type = result.GetType();
    meosType set_type = SetTypeMapping::GetMeosTypeFromAlias(target_type.GetAlias());

    auto input_data = FlatVector::GetData<string_t>(source);
    auto result_data = FlatVector::GetData<string_t>(result);

    for (idx_t i = 0; i < count; ++i) {
        if (FlatVector::IsNull(source, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        const std::string input_str = input_data[i].GetString();

        Set *s = set_in(input_str.c_str(), set_type);

        size_t size;
        uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);

        result_data[i] = StringVector::AddStringOrBlob(result, (const char*)(wkb), size);

        free(wkb);
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}


void SetTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    for (const auto &t : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterCastFunction(
            instance,
            t,                      
            LogicalType::VARCHAR,   
            SetBlobToText   
        ); // Blob to text
        ExtensionUtil::RegisterCastFunction(
            instance,
            LogicalType::VARCHAR, 
            t,                                    
            TextToSet   
        ); // text to blob
    }
}


// Set constructor from list 
static void SetFromList(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &list_input = args.data[0];
    auto meos_type = SetTypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());

    UnaryExecutor::Execute<list_entry_t, string_t>(
        list_input, result, args.size(),
        [&](list_entry_t list_entry) -> string_t {
            auto &child = ListVector::GetEntry(list_input);
            child.Flatten(args.size());  // ensure flat layout

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
                        throw InvalidInputException("Unsupported type in SetFromList");
                }
            }

            meosType base_type = settype_basetype(meos_type);
            Set *s = set_make_free(values, (int)length, base_type, true);
            
            size_t size;
            uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
            string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);

            free(wkb);
            free(s);                        
            return blob;
        });
}
void SetTypes::RegisterSetConstructors(DatabaseInstance &db) {
    for (auto &t : SetTypes::AllTypes()) {
        auto child_type = SetTypeMapping::GetChildType(t); 
        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("set", {LogicalType::LIST(child_type)}, t, SetFromList)                    
        );
    }    
}

// Conversion function: base type -> set 
static void SetFromBase(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<int32_t, string_t>(
                input, result, args.size(),
                [&](int32_t val) -> string_t {
                    Set *s = value_set(Datum(val), T_INT4);                    
                    size_t size;
                    uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
                    string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);
                    free(wkb); free(s);
                    return blob;         
                });
            break;
        
        case T_INT8:
            UnaryExecutor::Execute<int64_t, string_t>(
                input, result, args.size(),
                [&](int64_t val) -> string_t {
                    Set *s = value_set(Datum(val), T_INT8);
                    size_t size;
                    uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
                    string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);
                    free(wkb); free(s);
                    return blob;            
                });
            break;
        
        case T_FLOAT8:
            UnaryExecutor::Execute<double, string_t>(
                input, result, args.size(),
                [&](double val) -> string_t {
                    Set *s = value_set(Float8GetDatum(val), T_FLOAT8);
                    size_t size;
                    uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
                    string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);
                    free(wkb); free(s);
                    return blob;    
                });
            break;
    

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t val) -> string_t {
                    std::string str = val.GetString();
                    size_t len = str.size();

                    text *txt = (text *)malloc(VARHDRSZ + len);
                    SET_VARSIZE(txt, VARHDRSZ + len);
                    memcpy(VARDATA(txt), str.c_str(), len);

                    Set *s = value_set(PointerGetDatum(txt), T_TEXT);

                    size_t size;
                    uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
                    string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);
                    free(wkb); free(s);
                    return blob;    
                });
            break;
        
        case T_DATE:
            UnaryExecutor::Execute<date_t, string_t>(
                input, result, args.size(),
                [&](date_t val) -> string_t {                    
                    Set *s = value_set(Datum((int32_t)ToMeosDate(val)), T_DATE);
                    size_t size;
                    uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
                    string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);
                    free(wkb); free(s);
                    return blob;    
                });
            break;
        

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<timestamp_t, string_t>(
                input, result, args.size(),
                [&](timestamp_t val) -> string_t {                    
                    Set *s = value_set(Datum(ToMeosTimestamp(val)), T_TIMESTAMPTZ);
                    size_t size;
                    uint8_t *wkb = set_as_wkb(s, WKB_EXTENDED, &size);
                    string_t blob = StringVector::AddStringOrBlob(result, (const char *)wkb, size);
                    free(wkb); free(s);
                    return blob;    
                });
            break;
        
        default:
            throw NotImplementedException("SetFromBase: unsupported base type for set conversion");
    }
}

void SetTypes::RegisterSetConversion(DatabaseInstance &db) {
    for (auto &t : SetTypes::AllTypes()) {
        auto child_type = SetTypeMapping::GetChildType(t); 
        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("set", {child_type}, t, SetFromBase)                    
        );
    }    
}

// //memSize
// static void SetMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
//     auto &input = args.data[0];

//     UnaryExecutor::Execute<string_t, int32_t>(
//         input, result, args.size(),
//         [&](string_t input_str) -> int32_t {            
//             auto meos_type = SetTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());            
//             Set *s = set_in(input_str.GetString().c_str(), meos_type);
//             int size = VARSIZE_ANY(s);  // Get memory size
//             free(s);
//             return size;
//         });
// }

// void SetTypes::RegisterSetMemSize(DatabaseInstance &db) {
//     for (auto &set_type : SetTypes::AllTypes()) {
//         ExtensionUtil::RegisterFunction(
//             db, ScalarFunction(
//                 "memSize",
//                 {set_type},
//                 LogicalType::INTEGER,
//                 SetMemSize));
//     }
// }

//numValue
static void SetNumValues(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_blob) -> int32_t {
            const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
            size_t size = input_blob.GetSize();
            Set *s = set_from_wkb(wkb_data, size);
            if (!s) {
                throw InvalidInputException("Failed to parse WKB blob as Set");
            }

            int count = set_num_values(s);
            free(s);
            return count;
        });
}


void SetTypes::RegisterSetNumValues(DatabaseInstance &db){
    for (auto &set_type : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction(
                "numValues",
                {set_type},
                LogicalType::INTEGER,
                SetNumValues));
    }
}

//startValue 
static void SetStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<string_t, int32_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int32_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_start_value(s);
                    free(s);
                    return int32(d);
                });
            break;

        case T_INT8:
            UnaryExecutor::Execute<string_t, int64_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int64_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_start_value(s);
                    free(s);
                    return int64(d);
                });
            break;

        case T_FLOAT8:
            UnaryExecutor::Execute<string_t, double>(
                input, result, args.size(),
                [&](string_t input_blob) -> double {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_start_value(s);
                    free(s);
                    return DatumGetFloat8(d);
                });
            break;

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> string_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
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
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_start_value(s);
                    free(s);
                    return date_t(int32(FromMeosDate(d)));
                });
            break;

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<string_t, timestamp_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> timestamp_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_start_value(s);
                    free(s);
                    return timestamp_t(int64(FromMeosTimestamp(d)));
                });
            break;

        default:
            throw NotImplementedException("startValue: Unsupported set base type.");
    }
}

void SetTypes::RegisterSetStartValue(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {        
        auto child_type = SetTypeMapping::GetChildType(set_type); 

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("startValue", {set_type}, child_type, SetStartValue)
        );
    }
}

//endValue 
static void SetEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = SetTypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<string_t, int32_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int32_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_end_value(s);
                    free(s);
                    return int32(d);
                });
            break;

        case T_INT8:
            UnaryExecutor::Execute<string_t, int64_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> int64_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_end_value(s);
                    free(s);
                    return int64(d);
                });
            break;

        case T_FLOAT8:
            UnaryExecutor::Execute<string_t, double>(
                input, result, args.size(),
                [&](string_t input_blob) -> double {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_end_value(s);
                    free(s);
                    return DatumGetFloat8(d);
                });
            break;

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> string_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
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
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_end_value(s);
                    free(s);
                    return date_t(int32(FromMeosDate(d)));
                });
            break;

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<string_t, timestamp_t>(
                input, result, args.size(),
                [&](string_t input_blob) -> timestamp_t {
                    const uint8_t *wkb_data = (const uint8_t *)input_blob.GetData();
                    size_t size = input_blob.GetSize();
                    Set *s = set_from_wkb(wkb_data, size);
                    if (!s) {
                        throw InvalidInputException("Failed to parse WKB blob as Set");
                    }
                    Datum d = set_end_value(s);
                    free(s);
                    return timestamp_t(int64(FromMeosTimestamp(d)));
                });
            break;

        default:
            throw NotImplementedException("startValue: Unsupported set base type.");
    }
}

void SetTypes::RegisterSetEndValue(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {        
        auto child_type = SetTypeMapping::GetChildType(set_type); 

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("endValue", {set_type}, child_type, SetEndValue)
        );
    }
}

// valueN
static void SetValueN(DataChunk &args, ExpressionState &state, Vector &result_vec) {
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
        const uint8_t *wkb = (const uint8_t *)blob.GetData();
        size_t size = blob.GetSize();

        Set *s = set_from_wkb(wkb, size);

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

void SetTypes::RegisterSetValueN(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {
        LogicalType base_type = SetTypeMapping::GetChildType(set_type);
        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("valueN", {set_type, LogicalType::INTEGER}, base_type, SetValueN)
        );
    }
}


} // namespace duckdb
