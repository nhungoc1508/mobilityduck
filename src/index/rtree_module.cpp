#include "meos_wrapper_simple.hpp"

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "index/rtree_module.hpp"
#include "geo/stbox.hpp"
#include "index/rtree_index_create_physical.hpp"


namespace duckdb {

//------------------------------------------------------------------------------
// RTreeIndex Implementation with MEOS RTree Integration
//------------------------------------------------------------------------------

RTreeIndex::RTreeIndex(const string &name, IndexConstraintType constraint_type,
                       const vector<column_t> &column_ids, TableIOManager &table_io_manager,
                       const vector<unique_ptr<Expression>> &unbound_expressions,
                       AttachedDatabase &db,
                       const case_insensitive_map_t<Value> &options,
                       const IndexStorageInfo &info)
    : BoundIndex(name, TYPE_NAME, constraint_type, column_ids, table_io_manager, 
                unbound_expressions, db), options_(options), rtree_(nullptr) {
    
    
    // Create RTree specifically for integer stboxs (stbox)
    rtree_ = rtree_create_stbox();
    if (!rtree_) {
        throw InternalException("Failed to create MEOS RTree for stbox");
    }
    function_matcher = MakeFunctionMatcher();
}

class RTreeIndexScanState final : public IndexScanState {
public:
    STBox query_stbox;
    vector<row_t> search_results;
    idx_t current_position = 0;
    bool initialized = false;
};

RTreeIndex::~RTreeIndex() {
    if (rtree_) {
        rtree_free(rtree_);
        rtree_ = nullptr;
    }
    // Note: Don't call meos_finalize here as other instances might be using MEOS
}

PhysicalOperator &RTreeIndex::CreatePlan(PlanIndexInput &input) {
    auto &create_index = input.op;
    auto &planner = input.planner;

    // Skip all validation - just create the index
    
    // Simple projection
    vector<LogicalType> new_column_types;
    vector<unique_ptr<Expression>> select_list;
    
    for (auto &expression : create_index.expressions) {
        new_column_types.push_back(expression->return_type);
        select_list.push_back(std::move(expression));
    }
    
    new_column_types.emplace_back(LogicalType::ROW_TYPE);
    select_list.push_back(
        make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE, create_index.info->scan_types.size() - 1));

    auto &projection = planner.Make<PhysicalProjection>(new_column_types, std::move(select_list), 
                                                       create_index.estimated_cardinality);
    projection.children.push_back(input.table_scan);

    // Create the index directly without filtering
    auto &physical_create_index = planner.Make<PhysicalCreateRTreeIndex>(
        create_index.types, create_index.table, create_index.info->column_ids, 
        std::move(create_index.info), std::move(create_index.unbound_expressions), 
        create_index.estimated_cardinality);
    
    physical_create_index.children.push_back(projection);
    return physical_create_index;
}

//------------------------------------------------------------------------------
// Core RTree Operations using MEOS
//------------------------------------------------------------------------------
#define NO_BBOX 10000

ErrorData RTreeIndex::Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) {
    fprintf(stderr, "Start Insert");
    if (!rtree_) {
        return ErrorData("RTree not initialized");
    }
    
    // Check if we have data to insert
    if (data.size() == 0 || data.ColumnCount() == 0) {
        return ErrorData(); // Nothing to insert
    }
    
    // Get the stbox column data
    auto &stbox_vector = data.data[1];  // Single column for stbox data
    auto row_data = FlatVector::GetData<row_t>(row_ids);


    
    // Ensure the vector is flattened for direct access
    if (stbox_vector.GetVectorType() != VectorType::FLAT_VECTOR) {
        stbox_vector.Flatten(data.size());
    }
    
    // Check if this is actually an stbox type or needs conversion
    auto vector_type = stbox_vector.GetType();
    
    // Allocate array of STBox structures
    boxes = (STBox*)malloc(sizeof(STBox) * data.size());
    fprintf(stderr, "STBox, boxes");
    
    for (idx_t i = 0; i < data.size(); i++) {
        if (FlatVector::IsNull(stbox_vector, i)) {
            continue; // Skip NULL values
        }
        
        STBox *box = nullptr;
        
        if (vector_type.id() == LogicalTypeId::BLOB || 
                   vector_type.GetAlias() == "stbox" || 
                   vector_type.GetAlias() == "STBOX") {
            auto blob_data = FlatVector::GetData<string_t>(stbox_vector)[i];

            std::string s = blob_data.GetString();
            fprintf(stderr, "Insert blob data %s\n", s.c_str());
            const uint8_t *stbox_data = reinterpret_cast<const uint8_t*>(blob_data.GetData());
            size_t stbox_size = blob_data.GetSize();
            
            fprintf(stderr, "RTree Insert: Processing stbox blob, size: %zu bytes\n", stbox_size);
            
            // Allocate memory for the stbox
            box = (STBox*)malloc(stbox_size);
            
            // Copy the blob data to create our own stbox instance
            memcpy(box, stbox_data, stbox_size);

            char *stbox_str = stbox_out(box,15);
            fprintf(stderr, "Insert STBOX %s\n", stbox_str);
            
            // CRITICAL FIX: Check and normalize SRID
            int32_t box_srid = stbox_srid(box);
            fprintf(stderr, "Insert STBox SRID: %d\n", box_srid);
            
            // Option 1: Ensure consistent SRID (set to 0 if different)
            if (box_srid != 0) {
                // Transform to SRID 0 (no specific projection)
                STBox *normalized_box = stbox_set_srid(box, 0);
                if (normalized_box) {
                    free(box);
                    box = normalized_box;
                    fprintf(stderr, "Normalized STBox to SRID 0\n");
                }
            }
        } 
        else { 
            fprintf(stderr, "Unknown type: id=%d, alias='%s', toString='%s'\n", 
                   (int)vector_type.id(), vector_type.GetAlias().c_str(), vector_type.ToString().c_str());
            continue;
        }
        
        if (box == nullptr) {
            continue; // Skip if we couldn't create the stbox
        }
        
        memcpy(&boxes[i], box, sizeof(STBox));
        
        rtree_insert(rtree_, &boxes[i], static_cast<int64_t>(row_data[i]));
        

        fprintf(stderr, "RTree Insert successful for row %zu, row_id: %lld\n", 
               i, static_cast<long long>(row_data[i]));
        free(box);
    }

    free(boxes);
    
    return ErrorData();
}

ErrorData RTreeIndex::BulkConstruct(STBox* boxes, const row_t* row_ids, idx_t count) {
    if (!rtree_) {
        return ErrorData("RTree not initialized");
    }
    
    for (idx_t i = 0; i < count; i++) {
        // Use your rtree's bulk insert or construction method
        rtree_insert(rtree_, &boxes[i], static_cast<int64_t>(row_ids[i]));
    }
    
    return ErrorData();
}

void RTreeIndex::Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) {
    // MEOS RTree doesn't have built-in delete operation
    // For now, mark as not implemented
    // In practice, you might need to rebuild the tree or implement a deletion strategy
    throw NotImplementedException("RTree deletion not implemented - consider rebuilding index");
}

ErrorData RTreeIndex::Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) {
    // Delegate to Insert for MEOS RTree
    return Insert(lock, entries, row_identifiers);
}

//------------------------------------------------------------------------------
// RTree Search Operations
//------------------------------------------------------------------------------
unique_ptr<IndexScanState> RTreeIndex::InitializeScan(const void* query_blob, size_t blob_size) const {
    
    fprintf(stderr, "RTreeIndex::InitializeScan\n");
    const uint8_t *stbox_data = reinterpret_cast<const uint8_t*>(query_blob);
    STBox *box = (STBox*)malloc(blob_size);
    memcpy(box, stbox_data, blob_size);
    char *input_stbox_str = stbox_out(box, 15);
    if (input_stbox_str) {
        fprintf(stderr, "Input Query STBOX: %s\n", input_stbox_str);
        free(input_stbox_str);
    } 
    else{
        fprintf(stderr, "ERROR: Input STBox is invalid - cannot convert to string\n");
    }

    auto state = make_uniq<RTreeIndexScanState>();
    
    memcpy(&state->query_stbox, box, sizeof(STBox));

    
    // Check and normalize query SRID to match indexed data
    int32_t query_srid = stbox_srid(&state->query_stbox);
    fprintf(stderr, "Query STBox SRID: %d\n", query_srid);
    
    if (query_srid != 0) {
        // Transform query to SRID 0 to match indexed data
        STBox *normalized_query = stbox_set_srid(&state->query_stbox, 0);
        if (normalized_query) {
            memcpy(&state->query_stbox, normalized_query, sizeof(STBox));
            free(normalized_query);
            fprintf(stderr, "Normalized query STBox to SRID 0\n");
        }
    }
    
    // Perform the search using the normalized query
    if (rtree_) {
        state->search_results = SearchStbox(&state->query_stbox);
        state->initialized = true;
    }
    
    state->current_position = 0;
    
    fprintf(stderr, "RTree InitializeScan: Found %zu matching entries\n", 
            state->search_results.size());
    
    return std::move(state);
}

idx_t RTreeIndex::Scan(IndexScanState &state, Vector &result) const {
    auto &sstate = state.Cast<RTreeIndexScanState>();
    
    if (!sstate.initialized || sstate.search_results.empty()) {
        return 0;
    }
    
    // Get the row_ids data array from result vector
    const auto row_ids = FlatVector::GetData<row_t>(result);
    
    idx_t output_idx = 0;
    const idx_t max_output = STANDARD_VECTOR_SIZE;
    
    // Copy results from our search_results vector to the output
    while (sstate.current_position < sstate.search_results.size() && 
           output_idx < max_output) {
        
        row_ids[output_idx] = sstate.search_results[sstate.current_position];
        output_idx++;
        sstate.current_position++;
    }
    
    fprintf(stderr, "RTree Scan: Returning %zu row IDs, position: %zu/%zu\n",
            output_idx, sstate.current_position, sstate.search_results.size());
    
    return output_idx;
}


vector<row_t> RTreeIndex::SearchStbox(const STBox *query_stbox) const {
    fprintf(stderr, "SearchStbox\n");
    vector<row_t> results;
    
    if (!rtree_ || !query_stbox) {
        return results;
    }

    char *stbox_str = stbox_out(query_stbox,15);

    // Add SRID checking and normalization
    fprintf(stderr, "STBOX: %s\n", stbox_str);
    fprintf(stderr, "Query SRID: %d\n", stbox_srid(query_stbox));

    int count = 0;
    int *ids = nullptr;
    
    try {
        // Note: rtree_search expects const void* for query parameter
        ids = rtree_search(rtree_, (const void*)query_stbox, &count);
        
        if (ids && count > 0) {
            results.reserve(count);
            for (int i = 0; i < count; i++) {
                results.push_back(static_cast<row_t>(ids[i]));
            }
        }
    } catch (...) {
        fprintf(stderr, "Exception during rtree_search - likely SRID mismatch\n");
        // Handle any exceptions during search
    }
    
    // Clean up
    if (ids) {
        free(ids); // Free the array allocated by rtree_search
    }
    
    return results;
}

//------------------------------------------------------------------------------
// Required BoundIndex Interface Methods
//------------------------------------------------------------------------------

void RTreeIndex::CommitDrop(IndexLock &index_lock) {
    if (rtree_) {
        rtree_free(rtree_);
        rtree_ = nullptr;
    }
}

bool RTreeIndex::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
    // MEOS RTree doesn't have built-in merge operation
    return false;
}

void RTreeIndex::Vacuum(IndexLock &lock) {
    // Could potentially rebuild the tree for better performance
    // For now, no-op
}

idx_t RTreeIndex::GetInMemorySize(IndexLock &state) {
    // Since RTree is opaque, we can't access internal structure
    // Return estimated size or implement a size function in MEOS
    return rtree_ ? 1024 : 0; // Placeholder
}

string RTreeIndex::VerifyAndToString(IndexLock &state, const bool only_verify) {
    if (!rtree_) {
        return "Stbox R-tree Index (not initialized)";
    }
    
    // Since RTree is opaque, we can't access dims directly
    // You might need a MEOS function to get tree properties
    return "Stbox R-tree Index (MEOS-based)";
}

void RTreeIndex::VerifyAllocations(IndexLock &lock) {
    // Verify RTree structure integrity
    // Since RTree is opaque, limited verification possible
    // Could implement tree validation function in MEOS if needed
}

string RTreeIndex::GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
                                               DataChunk &input) {
    return "Stbox R-tree constraint violation (interval index does not support constraints)";
}

bool RTreeIndex::TryMatchDistanceFunction(const unique_ptr<Expression> &expr,
                                         vector<reference<Expression>> &bindings) const {
    return function_matcher->Match(*expr, bindings);
}

unique_ptr<ExpressionMatcher> RTreeIndex::MakeFunctionMatcher() const {
    // Create matcher for the && (overlaps) operator
    unordered_set<string> overlap_functions = {"&&"};

    auto matcher = make_uniq<FunctionExpressionMatcher>();
    matcher->function = make_uniq<ManyFunctionMatcher>(overlap_functions);
    matcher->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::BOUND_FUNCTION);
    matcher->policy = SetMatcher::Policy::UNORDERED;

    // Left operand: STBOX type
    auto lhs_matcher = make_uniq<ExpressionMatcher>();
    lhs_matcher->type = make_uniq<SpecificTypeMatcher>(StboxType::STBOX()); // Assuming STBOX is stored as BLOB
    matcher->matchers.push_back(std::move(lhs_matcher));

    // Right operand: STBOX type  
    auto rhs_matcher = make_uniq<ExpressionMatcher>();
    rhs_matcher->type = make_uniq<SpecificTypeMatcher>(StboxType::STBOX()); // Assuming STBOX is stored as BLOB
    matcher->matchers.push_back(std::move(rhs_matcher));

    return std::move(matcher);
}

//------------------------------------------------------------------------------
// Module Registration
//------------------------------------------------------------------------------

void RTreeModule::RegisterRTreeIndex(DatabaseInstance &db) {

    IndexType index_type;
    
    index_type.name = RTreeIndex::TYPE_NAME;
    index_type.create_instance = RTreeIndex::Create;
    index_type.create_plan = RTreeIndex::CreatePlan;

    // Register scan option - fix the parameter type
    db.config.AddExtensionOption("rtree_search_method",
                                 "search method for RTree indexes (overlap, contains, etc.)",
                                 LogicalType::VARCHAR, Value("overlap"));
    
    db.config.GetIndexTypes().RegisterIndexType(index_type);
}

} // namespace duckdb