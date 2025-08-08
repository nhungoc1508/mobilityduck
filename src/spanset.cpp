#include "spanset.hpp"
#include "span.hpp"
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

#define DEFINE_SPAN_SET_TYPE(NAME)                                        \
    LogicalType SpansetTypes::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::BLOB);             \
        type.SetAlias(#NAME);                                        \
        return type;                                                 \
    }

DEFINE_SPAN_SET_TYPE(intspanset)
DEFINE_SPAN_SET_TYPE(bigintspanset)
DEFINE_SPAN_SET_TYPE(floatspanset)
DEFINE_SPAN_SET_TYPE(datespanset)
DEFINE_SPAN_SET_TYPE(tstzspanset)

#undef DEFINE_SET_TYPE

void SpansetTypes::RegisterTypes(DatabaseInstance &db) {
    ExtensionUtil::RegisterType(db, "intspanset", intspanset());
    ExtensionUtil::RegisterType(db, "bigintspanset", bigintspanset());
    ExtensionUtil::RegisterType(db, "floatspanset", floatspanset());    
    ExtensionUtil::RegisterType(db, "datespanset", datespanset());
    ExtensionUtil::RegisterType(db, "tstzspanset", tstzspanset());    
}

const std::vector<LogicalType> &SpansetTypes::AllTypes() {
    static std::vector<LogicalType> types = {
        intspanset(),
        bigintspanset(),
        floatspanset(),        
        datespanset(),
        tstzspanset()
    };
    return types;
}

meosType SpansetTypeMapping::GetMeosTypeFromAlias(const std::string &alias) {
    static const std::unordered_map<std::string, meosType> alias_to_type = {
        {"intspanset", T_INTSPANSET},
        {"bigintspanset", T_BIGINTSPANSET},
        {"floatspanset", T_FLOATSPANSET},        
        {"datespanset", T_DATESPANSET},
        {"tstzspanset", T_TSTZSPANSET}                
    };

    auto it = alias_to_type.find(alias);
    if (it != alias_to_type.end()) {
        return it->second;
    } else {
        return T_UNKNOWN;
    }
}

LogicalType SpansetTypeMapping::GetChildType(const LogicalType &type) {
    auto alias = type.ToString();        
    if (alias == "intspanset") return SpanTypes::INTSPAN();
    if (alias == "bigintspanset") return SpanTypes::BIGINTSPAN();
    if (alias == "floatspanset") return SpanTypes::FLOATSPAN();    
    if (alias == "datespanset") return SpanTypes::DATESPAN();
    if (alias == "tstzspanset") return SpanTypes::TSTZSPAN();   
    throw NotImplementedException("GetChildType: unsupported alias: " + alias); 
}

LogicalType SpansetTypeMapping::GetSetType(const LogicalType &type) {
    auto alias = type.ToString();        
    if (alias == "intspanset") return SetTypes::intset();
    if (alias == "bigintspanset") return SetTypes::bigintset();
    if (alias == "floatspanset") return SetTypes::floatset();    
    if (alias == "datespanset") return SetTypes::dateset();
    if (alias == "tstzspanset") return SetTypes::tstzset();
    throw NotImplementedException("GetChildType: unsupported alias: " + alias);
}

LogicalType SpansetTypeMapping::GetBaseType(const LogicalType &type) {
    auto alias = type.ToString();
    if (alias == "intspanset") return LogicalType::INTEGER;
    if (alias == "bigintspanset") return LogicalType::BIGINT;
    if (alias == "floatspanset") return LogicalType::DOUBLE;    
    if (alias == "datespanset") return LogicalType::DATE;
    if (alias == "tstzspanset") return LogicalType::TIMESTAMP_TZ; 
    throw NotImplementedException("GetChildType: unsupported alias: " + alias);
}
//AsText
void SpansetFunctions::Spanset_as_text(DataChunk &args, ExpressionState &state, Vector &result) {
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

        SpanSet *s = (SpanSet *)malloc(size);
        memcpy(s, data, size);

        char *cstr = spanset_out(s, digits);
        auto str = StringVector::AddString(result, cstr);
        FlatVector::GetData<string_t>(result)[i] = str;

        free(s);
        free(cstr);
    }
}


// Cast to text 
bool SpansetFunctions::Spanset_to_text(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
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

        SpanSet *s = (SpanSet*)malloc(size);
        memcpy(s, data, size);        

        char *cstr = spanset_out(s, 15);  
        result_data[i] = StringVector::AddString(result, cstr);

        free(cstr);
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}

bool SpansetFunctions::Text_to_spanset(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {    
    source.Flatten(count);
    result.SetVectorType(VectorType::FLAT_VECTOR);

    auto target_type = result.GetType();
    meosType spanset_type = SpansetTypeMapping::GetMeosTypeFromAlias(target_type.GetAlias());

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t input_str) -> string_t {
            std::string str = input_str.GetString();

            SpanSet *s = spanset_in(str.c_str(), spanset_type);            

            string_t result_blob = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
            free(s);
            return result_blob;
        }
    );

    return true;
}


void SpansetTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    for (const auto &spanset_type : SpansetTypes::AllTypes()) {
        ExtensionUtil::RegisterCastFunction(
            instance,
            spanset_type,                      
            LogicalType::VARCHAR,   
            SpansetFunctions::Spanset_to_text   
        ); // Blob to text
        ExtensionUtil::RegisterCastFunction(
            instance,
            LogicalType::VARCHAR, 
            spanset_type,                                    
            SpansetFunctions::Text_to_spanset   
        ); // text to blob
        
        auto base_type = SpansetTypeMapping::GetBaseType(spanset_type);
        ExtensionUtil::RegisterCastFunction(
            instance,
            base_type,
            spanset_type,
            SpansetFunctions::Value_to_spanset
        );

        auto set_type = SpansetTypeMapping::GetSetType(spanset_type);        
        ExtensionUtil::RegisterCastFunction(
            instance,
            set_type,
            spanset_type,
            SpansetFunctions::Set_to_spanset
        );
    }
}

// Constructor 
void SpansetFunctions::Spanset_constructor(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &list_input = args.data[0];
    auto meos_type = SpansetTypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());

    UnaryExecutor::Execute<list_entry_t, string_t>(
        list_input, result, args.size(),
        [&](list_entry_t list_entry) -> string_t {
            auto &child = ListVector::GetEntry(list_input);
            child.Flatten(args.size());

            idx_t offset = list_entry.offset;
            idx_t length = list_entry.length;

            Span *spans = (Span *)malloc(sizeof(Span) * length);
            if (!spans) throw std::bad_alloc();

            for (idx_t i = 0; i < length; ++i) {
                idx_t idx = offset + i;
                auto blob_val = child.GetValue(idx);

                if (blob_val.IsNull()) {
                    free(spans);
                    throw InvalidInputException("Null span in input list");
                }

                auto blob_str = blob_val.GetValueUnsafe<string_t>(); 
                auto *bytes = (const uint8_t *)blob_str.GetData();
                auto size = blob_str.GetSize();

                if (size != sizeof(Span)) {
                    free(spans);
                    throw InvalidInputException("Invalid span blob size");
                }

                memcpy(&spans[i], bytes, size);
            }


            SpanSet *sset = spanset_make_free(spans, (int)length, meos_type, true);
            size_t size = VARSIZE(sset);
            string_t blob = StringVector::AddStringOrBlob(result, (const char *)sset, size);

            free(sset);
            return blob;
        }
    );
}

// Conversion function: base type -> spanset 
bool SpansetFunctions::Value_to_spanset(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {    
    auto target_type = result.GetType();
    meosType spanset_type = SpansetTypeMapping::GetMeosTypeFromAlias(target_type.GetAlias());
    meosType span_type = spansettype_spantype(spanset_type);
    meosType base_type = spantype_basetype(span_type);
    source.Flatten(count);

    switch (base_type) {
        case T_INT4: {
            auto input = FlatVector::GetData<int32_t>(source);
            auto output = FlatVector::GetData<string_t>(result);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) {
                    FlatVector::SetNull(result, i, true);
                    continue;
                }
                SpanSet *s = value_spanset(Datum(input[i]), T_INT4);
                output[i] = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
                free(s);
            }
            break;
        }
        case T_INT8: {
            auto input = FlatVector::GetData<int64_t>(source);
            auto output = FlatVector::GetData<string_t>(result);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) {
                    FlatVector::SetNull(result, i, true);
                    continue;
                }
                SpanSet *s = value_spanset(Datum(input[i]), T_INT8);
                output[i] = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
                free(s);
            }
            break;
        }
        case T_FLOAT8: {
            auto input = FlatVector::GetData<double>(source);
            auto output = FlatVector::GetData<string_t>(result);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) {
                    FlatVector::SetNull(result, i, true);
                    continue;
                }
                SpanSet *s = value_spanset(Float8GetDatum(input[i]), T_FLOAT8);
                output[i] = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
                free(s);
            }
            break;
        }
        case T_DATE: {
            auto input = FlatVector::GetData<date_t>(source);
            auto output = FlatVector::GetData<string_t>(result);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) {
                    FlatVector::SetNull(result, i, true);
                    continue;
                }
                int32_t days = (int32_t)ToMeosDate(input[i]);
                SpanSet *s = value_spanset(Datum(days), T_DATE);
                output[i] = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
                free(s);
            }
            break;
        }
        case T_TIMESTAMPTZ: {
            auto input = FlatVector::GetData<timestamp_t>(source);
            auto output = FlatVector::GetData<string_t>(result);
            for (idx_t i = 0; i < count; ++i) {
                if (FlatVector::IsNull(source, i)) {
                    FlatVector::SetNull(result, i, true);
                    continue;
                }
                timestamp_t ts = input[i];
                SpanSet *s = value_spanset(Datum(ToMeosTimestamp(ts)), T_TIMESTAMPTZ);
                output[i] = StringVector::AddStringOrBlob(result, (const char *)s, VARSIZE(s));
                free(s);
            }
            break;
        }
        default:
            throw NotImplementedException("SetConversion: unsupported base type for conversion to set");
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}

// Conversion: set -> spanset 
bool SpansetFunctions::Set_to_spanset(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {    
    source.Flatten(count);
    result.SetVectorType(VectorType::FLAT_VECTOR);

    UnaryExecutor::Execute<string_t, string_t>(
        source, result, count,
        [&](string_t blob) -> string_t {
            const uint8_t *bytes = (const uint8_t *)blob.GetData();
            size_t size = blob.GetSize();            
            Set *s = (Set *)malloc(size);
            memcpy(s, bytes, size);            

            SpanSet *spanset = set_spanset(s);
            free(s);

            size_t result_size = VARSIZE(spanset);
            string_t result_blob = StringVector::AddStringOrBlob(result, (const char *)spanset, result_size);
            free(spanset);

            return result_blob;
        }
    );

    return true;
}

void SpansetTypes::RegisterScalarFunctions(DatabaseInstance &db) {    
    for (const auto &spanset_type : SpansetTypes::AllTypes()) {
        auto child_type = SpansetTypeMapping::GetChildType(spanset_type);         

        // Register: asText
        if (spanset_type == SpansetTypes::floatspanset()) {            
            ExtensionUtil::RegisterFunction( // asText(floatset)
                db, ScalarFunction("asText", {spanset_type}, LogicalType::VARCHAR, SpansetFunctions::Spanset_as_text)
            );
            
            ExtensionUtil::RegisterFunction( // asText(floatset, int)
                db, ScalarFunction("asText", {spanset_type, LogicalType::INTEGER}, LogicalType::VARCHAR, SpansetFunctions::Spanset_as_text)
            );
        } else {            
            ExtensionUtil::RegisterFunction( // All other set types
                db, ScalarFunction("asText", {spanset_type}, LogicalType::VARCHAR, SpansetFunctions::Spanset_as_text)
            );
        }

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("spanset", {{LogicalType::LIST(child_type)}}, spanset_type, SpansetFunctions::Spanset_constructor)                 
        );
        
    }
}



} // namespace duckdb
