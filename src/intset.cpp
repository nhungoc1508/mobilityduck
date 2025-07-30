// #include "intset.hpp"
// #include "duckdb/common/extension_type_info.hpp"
// #include "duckdb/function/scalar_function.hpp"
// #include "duckdb/main/extension_util.hpp"

// extern "C" {
//     #include <postgres.h>
//     #include <utils/timestamp.h>
//     #include <meos.h>
//     #include <meos_rgeo.h>
//     #include <meos_internal.h>    
// }

// namespace duckdb {

//     // --- DEFINE INTSET TYPE ---
//     LogicalType IntSetType::INTSET() {        
//         auto type = LogicalType(LogicalTypeId::VARCHAR);
// 	    type.SetAlias("INTSET");
//         return type;
//     }

//     void IntSetType::RegisterTypes(DatabaseInstance &db){
//         ExtensionUtil::RegisterType(db, "INTSET", IntSetType::INTSET());
//     }
 
//     static void IntSetFromString(DataChunk &args, ExpressionState &state, Vector &result) {
//         auto &input = args.data[0];
//         UnaryExecutor::Execute<string_t, string_t>(
//             input, result, args.size(),
//             [&](string_t str) {
//                 Set *s = set_in(str.GetString().c_str(), T_INTSET);
//                 char *cstr = set_out(s, 15);
//                 string output(cstr);
//                 free(s);
//                 free(cstr);
//                 return StringVector::AddString(result, output);
//             }
//         );
//     }

//     void IntSetType::RegisterIntSet(DatabaseInstance &db) {
//         ExtensionUtil::RegisterFunction(
//             db,
//             ScalarFunction("INTSET", 
//             {LogicalType::VARCHAR}, //in
//             IntSetType::INTSET(), //out
//             IntSetFromString)
//         );
//     }


    // static void IntSetFromList(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &list_input = args.data[0];

    //     UnaryExecutor::Execute<list_entry_t, string_t>(
    //         list_input, result, args.size(),
    //         [&](list_entry_t list) -> string_t {
    //             auto &child = ListVector::GetEntry(list_input);
    //             idx_t offset = list.offset, length = list.length;

    //             // Allocate Datum array
    //             Datum *values = (Datum *)malloc(sizeof(Datum) * length);
    //             for (idx_t i = 0; i < length; ++i) {
    //                 values[i] = Int32GetDatum(child.GetValue(offset + i).GetValue<int32_t>());
    //             }

    //             // Build MEOS Set
    //             Set *s = set_make_free(values, (int)length, T_INT4, ORDER);

    //             // Serialize
    //             char *out_cstr = set_out(s, 15);
    //             string output(out_cstr);

    //             free(s);
    //             free(out_cstr);

    //             return StringVector::AddString(result, output);
    //         }
    //     );
    // }


    // void IntSetType::RegisterIntSetConstructor(DatabaseInstance &db){
    //     ExtensionUtil::RegisterFunction(
    //         db,            
        //     ScalarFunction("INTSET", 
        //                     {LogicalType::LIST(LogicalType::INTEGER)}, 
        //                     IntSetType::INTSET(), 
        //                     IntSetFromList)
        // );
    // }

    // // AsText function 

    // static void AsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &input = args.data[0];

    //     UnaryExecutor::Execute<string_t, string_t>(
    //         input, result, args.size(),
    //         [&](string_t input_str) -> string_t {
    //             Set *s = set_in(input_str.GetString().c_str(), T_INTSET);
    //             char *cstr = set_out(s, 15); // 15 = OUT_DEFAULT_DECIMAL_DIGITS
    //             string output(cstr);
    //             free(s);
    //             free(cstr);
    //             return StringVector::AddString(result, output);
    //         });
    // }

    // void IntSetType::RegisterIntSetAsText(DatabaseInstance &db){
    //     ExtensionUtil::RegisterFunction(
    //         db, ScalarFunction(
    //             "asText", 
    //             {IntSetType::INTSET()}, 
    //             LogicalType::VARCHAR, 
    //             AsTextFunction));        
    // }


    // // Conversion integer -> intset
    // static void SetFromInteger(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &input = args.data[0];

    //     UnaryExecutor::Execute<int32_t, string_t>(
    //         input, result, args.size(),
    //         [&](int32_t val) -> string_t {
    //             Datum d = Int32GetDatum(val);
    //             Set *s = value_set(d, T_INT4);         // ← MEOS function
    //             char *cstr = set_out(s, 15);
    //             string output(cstr);
    //             free(s);
    //             free(cstr);
    //             return StringVector::AddString(result, output);
    //         });
    // }

    // void IntSetType::RegisterIntToSet(DatabaseInstance &db){
    //     ExtensionUtil::RegisterFunction(
    //             db,
    //             ScalarFunction("Set", 
    //                             {LogicalType::INTEGER}, 
    //                             LogicalType::VARCHAR, 
    //                             SetFromInteger));

    // } 

    // // MemSize 
    // static void IntSetMemSize(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &input = args.data[0];

    //     UnaryExecutor::Execute<string_t, int32_t>(
    //         input, result, args.size(),
    //         [&](string_t input_str) -> int32_t {
    //             Set *s = set_in(input_str.GetString().c_str(), T_INTSET);
    //             int size = VARSIZE_ANY(s);  // or use MEOS-specific if exposed
    //             free(s);
    //             return size;
    //         });
    // }

    // void IntSetType::RegisterIntSetMemSize(DatabaseInstance &db){
    //     ExtensionUtil::RegisterFunction(
    //         db, ScalarFunction(
    //             "memSize", 
    //             {IntSetType::INTSET()}, 
    //             LogicalType::INTEGER, 
    //             IntSetMemSize));        
    // }

//     //numValue
//     static void IntSetNumValues(DataChunk &args, ExpressionState &state, Vector &result) {
//         auto &input = args.data[0];

//         UnaryExecutor::Execute<string_t, int32_t>(
//             input, result, args.size(),
//             [&](string_t input_str) -> int32_t {
//                 Set *s = set_in(input_str.GetString().c_str(), T_INTSET);
//                 int count = set_num_values(s);
//                 free(s);
//                 return count;
//             });
//     }

//     void IntSetType::RegisterIntSetNumValues(DatabaseInstance &db){
//         ExtensionUtil::RegisterFunction(
//             db, ScalarFunction(
//                 "numValues", 
//                 {IntSetType::INTSET()}, 
//                 LogicalType::INTEGER, 
//                 IntSetNumValues));        
//     }

    // //startValue
    // static void IntSetStartValue(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &input = args.data[0];

    //     UnaryExecutor::Execute<string_t, int32_t>(
    //         input, result, args.size(),
    //         [&](string_t input_str) -> int32_t {
    //             Set *s = set_in(input_str.GetString().c_str(), T_INTSET);
    //             Datum d = set_start_value(s);  // first element
    //             int32_t out = DatumGetInt32(d);
    //             free(s);
    //             return out;
    //         });
    // }

    // void IntSetType::RegisterIntSetStartValue(DatabaseInstance &db){
    //     ExtensionUtil::RegisterFunction(
    //         db, ScalarFunction(
    //             "startValue", 
    //             {IntSetType::INTSET()}, 
    //             LogicalType::INTEGER, 
    //             IntSetStartValue));        
    // }

    // endValue
    // static void IntSetEndValue(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &input = args.data[0];

    //     UnaryExecutor::Execute<string_t, int32_t>(
    //         input, result, args.size(),
    //         [&](string_t input_str) -> int32_t {
    //             Set *s = set_in(input_str.GetString().c_str(), T_INTSET);
    //             Datum d = set_end_value(s);  // last element
    //             int32_t out = DatumGetInt32(d);
    //             free(s);
    //             return out;
    //         });
    // }
//     void IntSetType::RegisterIntSetEndValue(DatabaseInstance &db){
//         ExtensionUtil::RegisterFunction(
//             db, ScalarFunction(
//                 "endValue", 
//                 {IntSetType::INTSET()}, 
//                 LogicalType::INTEGER, 
//                 IntSetEndValue));        
//     }

    // //valueN
    // static void IntSetValueN(DataChunk &args, ExpressionState &state, Vector &result) {
    //     auto &intset_vec = args.data[0];
    //     auto &index_vec = args.data[1];

    //     result.SetVectorType(VectorType::FLAT_VECTOR);
    //     auto result_data = FlatVector::GetData<int32_t>(result);
    //     auto &validity = FlatVector::Validity(result);

    //     for (idx_t i = 0; i < args.size(); i++) {
    //         // Default to NULL
    //         validity.SetInvalid(i);

    //         // Skip if either argument is NULL
    //         if (intset_vec.GetValue(i).IsNull() || index_vec.GetValue(i).IsNull()) {
    //             continue;
    //         }

    //         auto intset_str = StringValue::Get(intset_vec.GetValue(i));
    //         int32_t index = index_vec.GetValue(i).GetValue<int32_t>();
    //         if (index <= 0) {
    //             continue;
    //         }

    //         Set *s = set_in(intset_str.c_str(), T_INTSET);

    //         Datum val;
    //         bool found = set_value_n(s, index, &val);
    //         free(s);

    //         if (found) {
    //             result_data[i] = DatumGetInt32(val);
    //             validity.SetValid(i); 
    //         }
    //     }
    // }
    // void IntSetType::RegisterIntSetValueN(DatabaseInstance &db){
    //     ExtensionUtil::RegisterFunction(
    //         db, ScalarFunction(
    //             "valueN", 
    //             {IntSetType::INTSET(), LogicalType::INTEGER}, 
    //             LogicalType::INTEGER, 
    //             IntSetValueN));        
    // }

//     //getValues
//     void IntSetType::RegisterIntSetGetValues(DatabaseInstance &db) {
//         ExtensionUtil::RegisterFunction(
//             db,
//             ScalarFunction("getValues", 
//             {IntSetType::INTSET()}, //in
//             IntSetType::INTSET(), //out
//             IntSetFromString)
//         );
//     }

//     //shift

//     static void IntSetShift(DataChunk &args, ExpressionState &state, Vector &result) {
//         auto &intset_vec = args.data[0];
//         auto &shift_vec = args.data[1];

//         BinaryExecutor::Execute<string_t, int32_t, string_t>(
//             intset_vec, shift_vec, result, args.size(),
//             [&](string_t intset_str, int32_t shift) {
//                 Set *s = set_in(intset_str.GetString().c_str(), T_INTSET);
//                 Set *shifted = numset_shift_scale(s, Int32GetDatum(shift), 0, true, false);
//                 free(s);

//                 char *out = set_out(shifted, 15);
//                 string res(out);
//                 free(out);
//                 free(shifted);

//                 return StringVector::AddString(result, res);
//             });
//     }

//     void IntSetType::RegisterIntSetShift(DatabaseInstance &db) {
//         ExtensionUtil::RegisterFunction(
//             db,
//             ScalarFunction("shift", 
//             {IntSetType::INTSET(), LogicalType::INTEGER}, //in
//             IntSetType::INTSET(), //out
//             IntSetShift)
//         );
//     }

    //unnest
    // ----------- BindData -----------
    // struct IntSetUnnestBindData : public TableFunctionData {
    //     std::string intset_str;
    //     explicit IntSetUnnestBindData(std::string str) : intset_str(std::move(str)) {}
    // };

    // // ----------- Global State -----------
    // struct IntSetUnnestGlobalState : public GlobalTableFunctionState {
    //     idx_t idx = 0;
    //     std::vector<int32_t> values;
    // };

    // // ----------- Bind Function -----------
    // static unique_ptr<FunctionData> IntSetUnnestBind(ClientContext &context,
    //                                                 TableFunctionBindInput &input,
    //                                                 vector<LogicalType> &return_types,
    //                                                 vector<string> &names) {
    //     if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
    //         throw BinderException("unnest(INTSET): expects a non-null value");
    //     }
    //     auto str = input.inputs[0].ToString();
    //     return_types.emplace_back(LogicalType::INTEGER);
    //     names.emplace_back("unnest");
    //     return make_uniq<IntSetUnnestBindData>(std::move(str));
    // }

    // // ----------- Init Function -----------
    // static unique_ptr<GlobalTableFunctionState> IntSetUnnestInit(ClientContext &context,
    //                                                             TableFunctionInitInput &input) {
    //     auto &bind_data = input.bind_data->Cast<IntSetUnnestBindData>();

    //     // Allocate state object
    //     auto state = new IntSetUnnestGlobalState();

    //     // Parse input string to MEOS Set
    //     Set *s = set_in(bind_data.intset_str.c_str(), T_INTSET);
    //     int count = set_num_values(s);

    //     for (int i = 1; i <= count; i++) {
    //         Datum d;
    //         bool found = set_value_n(s, i, &d);
    //         if (found) {
    //             state->values.push_back(DatumGetInt32(d));
    //         }
    //     }
    //     free(s);

    //     // Return as base pointer
    //     return unique_ptr<GlobalTableFunctionState>(state);
    // }

    // // ----------- Exec Function -----------
    // static void IntSetUnnestExec(ClientContext &context, TableFunctionInput &input,
    //                             DataChunk &output) {
    //     auto &state = input.global_state->Cast<IntSetUnnestGlobalState>();
    //     auto result_data = FlatVector::GetData<int32_t>(output.data[0]);

    //     idx_t out_idx = 0;
    //     while (state.idx < state.values.size() && out_idx < STANDARD_VECTOR_SIZE) {
    //         result_data[out_idx++] = state.values[state.idx++];
    //     }
    //     output.SetCardinality(out_idx);
    // }

    // // ----------- Register -----------
    // void IntSetType::RegisterIntSetUnnest(DatabaseInstance &db) {
    //     TableFunction fn("intset_unnest",
    //                     {IntSetType::INTSET()}, // internal INTSET
    //                     IntSetUnnestExec,
    //                     IntSetUnnestBind,
    //                     IntSetUnnestInit);
    //     ExtensionUtil::RegisterFunction(db, fn);
    // }
// } // namespace duckdb