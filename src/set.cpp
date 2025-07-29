#include "set.hpp"
#include "types.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"


extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_rgeo.h>
    #include <meos_internal.h>    
}

namespace duckdb {

#define DEFINE_SET_TYPE(NAME)                                        \
    LogicalType SetTypes::NAME() {                                   \
        auto type = LogicalType(LogicalTypeId::VARCHAR);             \
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

static void GenericSetFromString(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = TypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());

    UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t str) {
            Set *s = set_in(str.GetString().c_str(), set_type);
            char *cstr = set_out(s, 15);
            std::string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        }
    );
}

void SetTypes::RegisterSet(DatabaseInstance &db) {    
    for (const auto &t : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction(t.ToString(), {LogicalType::VARCHAR}, t, GenericSetFromString)
        );
    }
}

//AsText
static void GenericAsText(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    auto meos_type = TypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());

    UnaryExecutor::Execute<string_t, string_t>(
        input, result, args.size(),
        [&](string_t input_str) -> string_t {
            Set *s = set_in(input_str.GetString().c_str(), meos_type);
            char *cstr = set_out(s, 15);
            string output(cstr);
            free(s);
            free(cstr);
            return StringVector::AddString(result, output);
        });
}

void SetTypes::RegisterSetAsText(DatabaseInstance &db) {    
    for (const auto &t : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction("asText", {t}, LogicalType::VARCHAR, GenericAsText)
        );
    }

}

// Set constructor from list 
static void GenericSetFromList(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &list_input = args.data[0];
    auto meos_type = TypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());

    UnaryExecutor::Execute<list_entry_t, string_t>(
        list_input, result, args.size(),
        [&](list_entry_t list_entry) -> string_t {
            auto &child = ListVector::GetEntry(list_input);
            idx_t offset = list_entry.offset;
            idx_t length = list_entry.length;

            Datum *values = (Datum *)malloc(sizeof(Datum) * length);

            for (idx_t i = 0; i < length; ++i) {
                auto val = child.GetValue(offset + i);
                switch (meos_type) {
                    case T_INTSET:{
                        values[i] = Int32GetDatum(val.GetValue<int32_t>());
                        break;
                    }
                    case T_BIGINTSET: {
                        values[i] = Int64GetDatum(val.GetValue<int64_t>());
                        break;
                    }
                    case T_FLOATSET:{
                        values[i] = Float8GetDatum(val.GetValue<double>());
                        break;
                    }
                    case T_TEXTSET:{
                        std::string s = val.ToString();
                        size_t len = s.size();

                        text *txt = (text *)malloc(VARHDRSZ + len);
                        SET_VARSIZE(txt, VARHDRSZ + len);
                        memcpy(VARDATA(txt), s.c_str(), len);
                        values[i] = CStringGetDatum(txt);                        
                        break;
                    }
                    case T_DATESET:{
                        auto d = val.GetValue<date_t>();
                        values[i] = DateADTGetDatum((int32_t)d);                        
                        break;
                    }
                    case T_TSTZSET:{                        
                        int64_t ts_val = val.GetValue<int64_t>();
                        values[i] = TimestampTzGetDatum(ts_val);                        
                        break;
                    }
                    default:
                        throw InvalidInputException("Unsupported type in SetFromList");
                }
            }
            meosType base_type = settype_basetype(meos_type);            
            Set *s = set_make_free(values, (int)length, base_type, ORDER);                        
            char *cstr = set_out(s, 15);
            string output(cstr);
            
            free(s);
            free(cstr);

            return StringVector::AddString(result, output);
        }
    );
}

void SetTypes::RegisterSetConstructors(DatabaseInstance &db) {
    for (auto &t : SetTypes::AllTypes()) {
        auto child_type = TypeMapping::GetChildType(t); 
        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("set", {LogicalType::LIST(child_type)}, t, GenericSetFromList)                    
        );
    }    
}

// Conversion function: base type -> set 
static void SetFromBase(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = TypeMapping::GetMeosTypeFromAlias(result.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<int32_t, string_t>(
                input, result, args.size(),
                [&](int32_t val) -> string_t {
                    Set *s = value_set(Int32GetDatum(val), T_INT4);
                    char *cstr = set_out(s, 15);
                    string output(cstr);
                    free(s); free(cstr);
                    return StringVector::AddString(result, output);
                });
            break;
        
        case T_INT8:
            UnaryExecutor::Execute<int64_t, string_t>(
                input, result, args.size(),
                [&](int64_t val) -> string_t {
                    Set *s = value_set(Int64GetDatum(val), T_INT8);
                    char *cstr = set_out(s, 15);
                    string output(cstr);
                    free(s); free(cstr);
                    return StringVector::AddString(result, output);
                });
            break;
        
        case T_FLOAT8:
            UnaryExecutor::Execute<double, string_t>(
                input, result, args.size(),
                [&](double val) -> string_t {
                    Set *s = value_set(Float8GetDatum(val), T_FLOAT8);
                    char *cstr = set_out(s, 15);
                    string output(cstr);
                    free(s); free(cstr);
                    return StringVector::AddString(result, output);
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

                    // Set *s = value_set(CStringGetDatum(val.GetString().c_str()), T_TEXT);
                    char *cstr = set_out(s, 15);
                    string output(cstr);
                    free(s); free(cstr);free(txt);
                    return StringVector::AddString(result, output);
                });
            break;
        
        case T_DATE:
            UnaryExecutor::Execute<date_t, string_t>(
                input, result, args.size(),
                [&](date_t val) -> string_t {                    
                    Set *s = value_set(DateADTGetDatum((int32_t)val), T_DATE);
                    char *cstr = set_out(s, 15);
                    string output(cstr);
                    free(s); free(cstr);
                    return StringVector::AddString(result, output);
                });
            break;
        

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<timestamp_t, string_t>(
                input, result, args.size(),
                [&](timestamp_t val) -> string_t {                    
                    Set *s = value_set(TimestampTzGetDatum((int64_t)val), T_TIMESTAMPTZ);
                    char *cstr = set_out(s, 15);
                    string output(cstr);
                    free(s); free(cstr);
                    return StringVector::AddString(result, output);
                });
            break;
        
        default:
            throw NotImplementedException("SetFromBase: unsupported base type for set conversion");
    }
}

void SetTypes::RegisterSetConversion(DatabaseInstance &db) {
    for (auto &t : SetTypes::AllTypes()) {
        auto child_type = TypeMapping::GetChildType(t); 
        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("set", {child_type}, t, SetFromBase)                    
        );
    }    
}

//memSize
static void SetMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_str) -> int32_t {            
            auto meos_type = TypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());            
            Set *s = set_in(input_str.GetString().c_str(), meos_type);
            int size = VARSIZE_ANY(s);  // Get memory size
            free(s);
            return size;
        });
}

void SetTypes::RegisterSetMemSize(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction(
                "memSize",
                {set_type},
                LogicalType::INTEGER,
                SetMemSize));
    }
}
//numValue
static void SetNumValues(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];

    UnaryExecutor::Execute<string_t, int32_t>(
        input, result, args.size(),
        [&](string_t input_str) -> int32_t {
            auto meos_type = TypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());            
            Set *s = set_in(input_str.GetString().c_str(), meos_type);
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
    auto set_type = TypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<string_t, int32_t>(
                input, result, args.size(),
                [&](string_t input_str) -> int32_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_start_value(s);
                    free(s);
                    return DatumGetInt32(d);
                });
            break;

        case T_INT8:
            UnaryExecutor::Execute<string_t, int64_t>(
                input, result, args.size(),
                [&](string_t input_str) -> int64_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_start_value(s);
                    free(s);
                    return DatumGetInt64(d);
                });
            break;

        case T_FLOAT8:
            UnaryExecutor::Execute<string_t, double>(
                input, result, args.size(),
                [&](string_t input_str) -> double {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_start_value(s);
                    free(s);
                    return DatumGetFloat8(d);
                });
            break;

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t input_str) -> string_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_start_value(s);
                    free(s);                    
                    // const char *cstr = DatumGetCString(d);
                    text *txt = (text *)DatumGetPointer(d);
                    int len = VARSIZE(txt) - VARHDRSZ;
                    string str(VARDATA(txt), len);
                    return StringVector::AddString(result, str); 
                });
            break;

        case T_DATE:
            UnaryExecutor::Execute<string_t, date_t>(
                input, result, args.size(),
                [&](string_t input_str) -> date_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_start_value(s);
                    free(s);
                    return date_t(DatumGetInt32(d));
                });
            break;

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<string_t, timestamp_t>(
                input, result, args.size(),
                [&](string_t input_str) -> timestamp_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_start_value(s);
                    free(s);
                    return timestamp_t(DatumGetInt64(d));
                });
            break;

        default:
            throw NotImplementedException("startValue: Unsupported set base type.");
    }
}

void SetTypes::RegisterSetStartValue(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {        
        auto child_type = TypeMapping::GetChildType(set_type); 

        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("startValue", {set_type}, child_type, SetStartValue)
        );
    }
}

//endValue 
static void SetEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto set_type = TypeMapping::GetMeosTypeFromAlias(input.GetType().ToString());
    auto base_type = settype_basetype(set_type);

    switch (base_type) {
        case T_INT4:
            UnaryExecutor::Execute<string_t, int32_t>(
                input, result, args.size(),
                [&](string_t input_str) -> int32_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_end_value(s);
                    free(s);
                    return DatumGetInt32(d);
                });
            break;

        case T_INT8:
            UnaryExecutor::Execute<string_t, int64_t>(
                input, result, args.size(),
                [&](string_t input_str) -> int64_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_end_value(s);
                    free(s);
                    return DatumGetInt64(d);
                });
            break;

        case T_FLOAT8:
            UnaryExecutor::Execute<string_t, double>(
                input, result, args.size(),
                [&](string_t input_str) -> double {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_end_value(s);
                    free(s);
                    return DatumGetFloat8(d);
                });
            break;

        case T_TEXT:
            UnaryExecutor::Execute<string_t, string_t>(
                input, result, args.size(),
                [&](string_t input_str) -> string_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
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
                [&](string_t input_str) -> date_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_end_value(s);
                    free(s);
                    return date_t(DatumGetInt32(d));
                });
            break;

        case T_TIMESTAMPTZ:
            UnaryExecutor::Execute<string_t, timestamp_t>(
                input, result, args.size(),
                [&](string_t input_str) -> timestamp_t {
                    Set *s = set_in(input_str.GetString().c_str(), set_type);
                    Datum d = set_end_value(s);
                    free(s);
                    return timestamp_t(DatumGetInt64(d));
                });
            break;

        default:
            throw NotImplementedException("startValue: Unsupported set base type.");
    }
}

void SetTypes::RegisterSetEndValue(DatabaseInstance &db) {
    for (auto &set_type : SetTypes::AllTypes()) {        
        auto child_type = TypeMapping::GetChildType(set_type); 

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

    const auto set_type = TypeMapping::GetMeosTypeFromAlias(set_vec.GetType().ToString());
    const auto base_type = settype_basetype(set_type);

    result_vec.SetVectorType(VectorType::FLAT_VECTOR);
    auto &validity = FlatVector::Validity(result_vec);

    for (idx_t i = 0; i < args.size(); i++) {
        validity.SetInvalid(i);

        if (set_vec.GetValue(i).IsNull() || index_vec.GetValue(i).IsNull()) continue;

        auto set_str = StringValue::Get(set_vec.GetValue(i));
        int32_t index = index_vec.GetValue(i).GetValue<int32_t>();
        if (index <= 0) continue;

        Set *s = set_in(set_str.c_str(), set_type);
        Datum d;
        bool found = set_value_n(s, index, &d);
        free(s);

        if (!found) continue;
        
        switch (base_type) {
            case T_INT4:
                FlatVector::GetData<int32_t>(result_vec)[i] = DatumGetInt32(d);
                break;
            case T_INT8:
                FlatVector::GetData<int64_t>(result_vec)[i] = DatumGetInt64(d);
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
                int32_t raw = DatumGetInt32(d);
                FlatVector::GetData<date_t>(result_vec)[i] = date_t(raw);
                break;
            }
            case T_TIMESTAMPTZ: {
                int64_t raw = DatumGetInt64(d);
                FlatVector::GetData<timestamp_t>(result_vec)[i] = timestamp_t(raw);
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
        LogicalType base_type = TypeMapping::GetChildType(set_type);
        ExtensionUtil::RegisterFunction(
            db,
            ScalarFunction("valueN", {set_type, LogicalType::INTEGER}, base_type, SetValueN)
        );
    }
}

//getValues
void SetTypes::RegisterSetGetValues(DatabaseInstance &db) {    
    for (const auto &t : SetTypes::AllTypes()) {
        ExtensionUtil::RegisterFunction(
            db, ScalarFunction("getValues", {t}, t, GenericSetFromString)
        );
    }
}

//Unnest

struct SetUnnestBindData : public TableFunctionData {
    std::string set_str;
    meosType set_type;
    LogicalType return_type;

    explicit SetUnnestBindData(string str, meosType t, LogicalType ret_type)
        : set_str(std::move(str)), set_type(t), return_type(std::move(ret_type)) {}
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
        throw BinderException("unnest(set): expects a non-null set");
    }

    auto set_str = input.inputs[0].ToString();    
    auto in_type = input.inputs[0].type();    
    auto duck_type = TypeMapping::GetChildType(in_type);    
    auto set_type = TypeMapping::GetMeosTypeFromAlias(in_type.ToString());

    return_types.emplace_back(duck_type);
    names.emplace_back("unnest");
    return make_uniq<SetUnnestBindData>(set_str, set_type, duck_type);
}

static unique_ptr<GlobalTableFunctionState> SetUnnestInit(ClientContext &context,
                                                          TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<SetUnnestBindData>();
    auto state = new SetUnnestGlobalState();

    Set *s = set_in(bind.set_str.c_str(), bind.set_type);
    int count = set_num_values(s);

    for (int i = 1; i <= count; i++) {
        Datum d;
        bool found = set_value_n(s, i, &d);
        if (!found) continue;

        switch (settype_basetype(bind.set_type)) {
            case T_INT4:      state->values.emplace_back(Value::INTEGER(DatumGetInt32(d))); break;
            case T_INT8:      state->values.emplace_back(Value::BIGINT(DatumGetInt64(d))); break;
            case T_FLOAT8:    state->values.emplace_back(Value::DOUBLE(DatumGetFloat8(d))); break;
            case T_TEXT: {     
                text *txt = (text *)DatumGetPointer(d);
                int len = VARSIZE(txt) - VARHDRSZ;
                std::string str(VARDATA(txt), len);
                state->values.emplace_back(Value(str));
                break;
            }
            case T_DATE:      state->values.emplace_back(Value::DATE(date_t(DatumGetInt32(d)))); break;
            case T_TIMESTAMPTZ: {                
                state->values.emplace_back(Value::TIMESTAMPTZ(timestamp_tz_t(DatumGetInt64(d)))); 
                break;
            }
            default:
                free(s);
                throw NotImplementedException("unnest(set): unsupported base type");
        }
    }

    free(s);
    return unique_ptr<GlobalTableFunctionState>(state);
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
