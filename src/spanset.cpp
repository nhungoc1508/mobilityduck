#include "spanset.hpp"
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


// Cast
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

    auto target_type = result.GetType();
    meosType set_type = SpansetTypeMapping::GetMeosTypeFromAlias(target_type.GetAlias());

    auto input_data = FlatVector::GetData<string_t>(source);
    auto result_data = FlatVector::GetData<string_t>(result);

    for (idx_t i = 0; i < count; ++i) {
        if (FlatVector::IsNull(source, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        const std::string input_str = input_data[i].GetString();
        SpanSet *s = nullptr;
        // if (set_type == T_TEXTSET && !input_str.empty() && input_str.front() != '{') {                           
        //     text *txt = (text *)malloc(VARHDRSZ + input_str.size());
        //     SET_VARSIZE(txt, VARHDRSZ + input_str.size());
        //     memcpy(VARDATA(txt), input_str.c_str(), input_str.size());

        //     s = value_set(PointerGetDatum(txt), T_TEXT);                
        // } else {
            s = spanset_in(input_str.c_str(), set_type);     
        // }               
        size_t total_size = VARSIZE(s); 
        result_data[i] = StringVector::AddStringOrBlob(result, (const char*)s, total_size);        
        free(s);
    }

    result.SetVectorType(VectorType::FLAT_VECTOR);
    return true;
}


void SpansetTypes::RegisterCastFunctions(DatabaseInstance &instance) {
    for (const auto &set_type : SpansetTypes::AllTypes()) {
        ExtensionUtil::RegisterCastFunction(
            instance,
            set_type,                      
            LogicalType::VARCHAR,   
            SpansetFunctions::Spanset_to_text   
        ); // Blob to text
        ExtensionUtil::RegisterCastFunction(
            instance,
            LogicalType::VARCHAR, 
            set_type,                                    
            SpansetFunctions::Text_to_spanset   
        ); // text to blob
        
        // auto base_type = SetTypeMapping::GetChildType(set_type);
        // ExtensionUtil::RegisterCastFunction(
        //     instance,
        //     base_type,
        //     set_type,
        //     SetFunctions::Value_to_set
        // );
    }
}

} // namespace duckdb
