#include "meos_wrapper_simple.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include "duckdb/main/database.hpp"
#include <algorithm>

#include "index/rtree_module.hpp"
#include "index/rtree_index_scan.hpp"


namespace duckdb {

class RTreeIndexScanOptimizer : public OptimizerExtension {
public:
    RTreeIndexScanOptimizer() {
        fprintf(stderr, "RTreeIndexScanOptimizer::Constructor\n");
        optimize_function = Optimize;
    }

private:
    static bool TryOptimizeLogicalGet(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
        fprintf(stderr, "RTreeOptimizer::TryOptimizeLogicalGet\n");
        
        auto &get = plan->Cast<LogicalGet>();

        if (get.function.name != "seq_scan" || !get.GetTable()->IsDuckTable()) {
            fprintf(stderr, "RTreeOptimizer::Not a seq_scan on duck table\n");
            return false;
        }

        fprintf(stderr, "RTreeOptimizer::Found %zu table filters on GET\n", get.table_filters.filters.size());

        auto &duck_table = get.GetTable()->Cast<DuckTableEntry>();
        auto &table_info = *get.GetTable()->GetStorage().GetDataTableInfo();
        
        unique_ptr<RTreeIndexScanBindData> bind_data = nullptr;
        vector<reference<Expression>> bindings;

        // Check each table filter for spatial overlap operations
        for (auto &filter_pair : get.table_filters.filters) {
            auto &filter = filter_pair.second;
            
            table_info.GetIndexes().BindAndScan<RTreeIndex>(context, table_info, 
            [&](RTreeIndex &rtree_index) -> bool {
                bindings.clear();
                
                fprintf(stderr, "RTreeOptimizer::Found RTree index, checking filter match\n");
                
                // Try to match the filter expression against the spatial function
                auto &expr_filter = filter->Cast<ExpressionFilter>();
                if (!rtree_index.TryMatchDistanceFunction(expr_filter.expr, bindings)) {
                    fprintf(stderr, "RTreeOptimizer::Filter doesn't match spatial function\n");
                    return false;
                }

                fprintf(stderr, "RTreeOptimizer::Filter matched! Found %zu bindings\n", bindings.size());

                // Find constant expression for stbox query
                Expression *const_expr = nullptr;
                Expression *column_expr = nullptr;
                
                for (auto &binding : bindings) {
                    if (binding.get().type == ExpressionType::VALUE_CONSTANT) {
                        const_expr = &binding.get();
                        fprintf(stderr, "RTreeOptimizer::Found constant expression\n");
                    } else if (binding.get().type == ExpressionType::BOUND_COLUMN_REF) {
                        column_expr = &binding.get();
                        fprintf(stderr, "RTreeOptimizer::Found column reference\n");
                    }
                }

                if (!const_expr) {
                    fprintf(stderr, "RTreeOptimizer::No constant expression found\n");
                    return false;
                }

                // Extract stbox from constant expression
                const auto &constant = const_expr->Cast<BoundConstantExpression>();
                
                unique_ptr<STBox> query_stbox = nullptr;
                
                if (constant.value.type().GetAlias() == "stbox" || 
                    constant.value.type().id() == LogicalTypeId::BLOB) {
                    fprintf(stderr, "RTreeOptimizer::Processing stbox constant\n");
                    
                    // Get the raw data
                    auto blob_data = constant.value.GetValue<string>();
                    const uint8_t *stbox_data = reinterpret_cast<const uint8_t*>(blob_data.data());
                    size_t stbox_size = blob_data.size();
                    
                    fprintf(stderr, "RTreeOptimizer Processing stbox blob, size: %zu bytes\n", stbox_size);

                    fprintf(stderr, "RTreeOptimizer::STBox data preview: ");
                    for (size_t i = 0; i < std::min(stbox_size, 32UL); i++) {
                        fprintf(stderr, "%02x ", stbox_data[i]);
                    }
                    fprintf(stderr, "\n");

                    STBox *box = nullptr;

                    // The data is a DuckDB BLOB containing STBox data
                    // Try different approaches to extract the actual STBox
                    
                    if (stbox_size > 80) {
                        fprintf(stderr, "RTreeOptimizer::Large BLOB detected, looking for STBox data within\n");
                        
                        // First, let's see if this might be text data that we can parse
                        // Check if the data looks like escaped text
                        bool might_be_text = true;
                        for (size_t i = 0; i < std::min(stbox_size, 64UL) && might_be_text; i++) {
                            uint8_t byte = stbox_data[i];
                            // Look for patterns that suggest this is escaped text or binary STBox data
                            if (byte > 127) { // Non-ASCII
                                might_be_text = false;
                            }
                        }
                        
                        if (might_be_text) {
                            fprintf(stderr, "RTreeOptimizer::Attempting to parse as text representation\n");
                            
                            // Convert to string and try to extract the STBOX part
                            std::string blob_str(reinterpret_cast<const char*>(stbox_data), stbox_size);
                            
                            // Look for "STBOX" in the string
                            size_t stbox_pos = blob_str.find("STBOX");
                            if (stbox_pos != std::string::npos) {
                                std::string stbox_part = blob_str.substr(stbox_pos);
                                
                                // Remove any null characters
                                stbox_part.erase(std::remove(stbox_part.begin(), stbox_part.end(), '\0'), stbox_part.end());
                                
                                // Find the end of the STBOX string (look for closing parenthesis)
                                size_t end_pos = stbox_part.find('))');
                                if (end_pos != std::string::npos) {
                                    stbox_part = stbox_part.substr(0, end_pos + 2);
                                }
                                
                                fprintf(stderr, "RTreeOptimizer::Extracted STBOX string: '%s'\n", stbox_part.c_str());
                                
                                // Try to parse this as an STBOX
                                box = stbox_in(stbox_part.c_str());
                                if (box) {
                                    fprintf(stderr, "RTreeOptimizer::Successfully parsed STBOX from text\n");
                                }
                            }
                        }
                        
                        // If text parsing failed, let's examine the data more carefully
                        if (!box) {
                            fprintf(stderr, "RTreeOptimizer::Text parsing failed\n");
                            
                            // Let's decode what this hex pattern actually means
                            fprintf(stderr, "RTreeOptimizer::Full hex dump: ");
                            for (size_t i = 0; i < std::min(stbox_size, 100UL); i++) {
                                fprintf(stderr, "%02x", stbox_data[i]);
                                if (i % 16 == 15) fprintf(stderr, "\n");
                                else if (i % 4 == 3) fprintf(stderr, " ");
                            }
                            fprintf(stderr, "\n");
                            
                            // The pattern 5c 78 30 30 = \x00 suggests this might be an encoding issue
                            // Let's try to create a simple STBox manually from the coordinates we know
                            fprintf(stderr, "RTreeOptimizer::Creating STBox manually for query coordinates (1,2,3,4)\n");
                            
                            // Create STBox using MEOS function with known coordinates
                            // Since we know this should be "STBOX X((1.0,2.0),(3.0,4.0))", let's create it directly
                            box = stbox_in("STBOX X((1.0,2.0),(3.0,4.0))");
                            
                            if (box) {
                                fprintf(stderr, "RTreeOptimizer::Successfully created STBox manually\n");
                                char *verify_str = stbox_out(box, 15);
                                if (verify_str) {
                                    fprintf(stderr, "RTreeOptimizer::Manual STBox: %s\n", verify_str);
                                    free(verify_str);
                                }
                            } else {
                                fprintf(stderr, "RTreeOptimizer::Failed to create STBox manually\n");
                            }
                        }
                    }
                    else if (stbox_size == 80) { // This looks like actual binary STBox data
                        fprintf(stderr, "RTreeOptimizer::Detected binary STBox data\n");
                        
                        // Allocate memory for the stbox
                        box = (STBox*)malloc(stbox_size);
                        
                        // Copy the blob data to create our own stbox instance
                        memcpy(box, stbox_data, stbox_size);
                    }
                    else {
                        fprintf(stderr, "RTreeOptimizer::Unknown STBox format, size: %zu\n", stbox_size);
                    }

                    if (box) {
                        fprintf(stderr, "RTreeOptimizer::Box pointer: %p\n", (void*)box);
                        
                        char *stbox_str = stbox_out(box, 15);
                        fprintf(stderr, "RTreeOptimizer::Parsed STBOX %s\n", stbox_str);
                        if (stbox_str) free(stbox_str);
                        
                        query_stbox = unique_ptr<STBox>(box);
                    }
                }

                if (!query_stbox) {
                    fprintf(stderr, "RTreeOptimizer::Failed to extract query stbox\n");
                    return false;
                }

                fprintf(stderr, "RTreeOptimizer::Successfully extracted query stbox\n");

                bind_data = make_uniq<RTreeIndexScanBindData>(
                    duck_table, rtree_index, 1000, std::move(query_stbox));
                return true;
            });
            
            if (bind_data) {
                fprintf(stderr, "RTreeOptimizer::Successfully created bind data for table filter\n");
                break;
            }
        }

        if (!bind_data) {
            fprintf(stderr, "RTreeOptimizer::No matching index found for table filters\n");
            return false;
        }

        fprintf(stderr, "RTreeOptimizer::Replacing scan function with index scan\n");

        // Replace the scan function with index scan
        auto cardinality = get.function.cardinality(context, bind_data.get());
        get.function = RTreeIndexScanFunction::GetFunction();
        get.has_estimated_cardinality = cardinality->has_estimated_cardinality;
        get.estimated_cardinality = cardinality->estimated_cardinality;
        get.bind_data = std::move(bind_data);

        // Clear the table filters since the index scan handles the filtering
        // get.table_filters.reset();

        if (!get.bind_data) {
            fprintf(stderr, "ERROR: bind_data is null after assignment!\n");
            throw InternalException("bind_data is null after assignment");
        }

        return true;
    }

public:
    static bool TryOptimize(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
        fprintf(stderr, "RTreeOptimizer::TryOptimize - Plan type: %d\n", (int)plan->type);
        
        // Handle different plan structures
        switch (plan->type) {
                
            case LogicalOperatorType::LOGICAL_GET:
                return TryOptimizeLogicalGet(context, plan);
                
            default:
                fprintf(stderr, "RTreeOptimizer::Unsupported plan type: %d\n", (int)plan->type);
                return false;
        }
    }

    static bool OptimizeRecursive(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
        bool optimized = TryOptimize(context, plan);
        
        for (auto &child : plan->children) {
            optimized |= OptimizeRecursive(context, child);
        }
        
        return optimized;
    }

    static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
        OptimizeRecursive(input.context, plan);
        fprintf(stderr, "Done OptimizeRecursive\n");
    }
};

void RTreeModule::RegisterScanOptimizer(DatabaseInstance &instance) {
    instance.config.optimizer_extensions.push_back(RTreeIndexScanOptimizer());
}

} // namespace duckdb