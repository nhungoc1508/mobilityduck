#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb {

class Index;

// // This is created by the optimizer rule
// struct RTreeIndexScanBindData final : public TableScanBindData {
// 	explicit RTreeIndexScanBindData(TableCatalogEntry &table, Index &index, idx_t limit,
// 	                               unsafe_unique_array<float> query)
// 	    : TableScanBindData(table), index(index), limit(limit), query(std::move(query)) {
// 	}

// 	//! The index to use
// 	Index &index;

// 	//! The limit of the scan
// 	idx_t limit;

// 	//! The query vector
// 	unsafe_unique_array<float> query;
// };


struct RTreeIndexScanBindData : public TableFunctionData {
    DuckTableEntry &table;
    RTreeIndex &index;
    idx_t limit;
    unique_ptr<STBox> query_stbox;  // Change from float* to STBox*
    
    RTreeIndexScanBindData(DuckTableEntry &table, RTreeIndex &index, idx_t limit, 
                           unique_ptr<STBox> query_stbox)
        : table(table), index(index), limit(limit), query_stbox(std::move(query_stbox)) {}
};

struct RTreeIndexScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb