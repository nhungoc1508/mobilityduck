# Temporal Type Benchmark Suite

This benchmark suite compares the performance of two temporal type implementations in MobilityDuck:

## **Type Implementations**

### **TINT (Pointer-based)**
- Uses raw pointers to MEOS temporal objects
- **Status**: ❌ **Not production-ready** - fails on table queries
- **Issue**: Pointer serialization/deserialization problems when stored in tables

### **TINT2 (String-based)** 
- Uses string serialization of temporal objects
- **Status**: ✅ **Production-ready** - works in all scenarios
- **Advantage**: Robust persistence and no memory management issues

## **Key Findings**

**TINT2 is the recommended implementation** because:
- ✅ Works for individual operations AND bulk operations
- ✅ Survives database storage/retrieval cycles
- ✅ No memory management issues
- ✅ More reliable and robust

**TINT has limitations**:
- ❌ Fails when querying from stored tables
- ❌ Pointer-based approach doesn't survive persistence
- ❌ Not suitable for production use

## **Benchmark Files**

### **1. C++ Benchmark** (`benchmark_temporal_types.cpp`)
- **Purpose**: Large-scale performance comparison
- **Data**: 100,000 rows per table
- **Operations**: Constructor, Cast, Value Extraction, Subtype, Timestamp, Sequences, Complex Operations, Bulk Operations
- **Output**: Min/avg/max timing statistics for each operation

### **2. Python SQL Benchmark** (`benchmark_sql_runner.py`) ⭐ **RECOMMENDED**
- **Purpose**: Automated SQL benchmark with timing collection
- **Data**: 100,000 rows per table
- **Operations**: getValue, tempSubtype, getTimestamp, sequences, GroupBy, Window functions
- **Output**: Comparison table and detailed JSON results
- **Advantage**: Automatic timing, error handling, and structured output

### **3. SQL Benchmark** (`benchmark_sql.sql`)
- **Purpose**: Manual SQL benchmark for direct DuckDB testing
- **Data**: 100,000 rows per table
- **Operations**: Same as Python benchmark
- **Output**: Query results to terminal, profiling to JSON file

## **How to Run**

### **1. Python SQL Benchmark (Recommended)**
```bash
# Run the automated benchmark
make -f Makefile.benchmark run-sql-python

# Or run directly
python3 benchmark_sql_runner.py
```

**Output**: 
- Progress indicators for each query
- Comparison table (TINT vs TINT2)
- Detailed statistics
- JSON file with complete results

### **2. C++ Benchmark**
```bash
# Build and run
make -f Makefile.benchmark benchmark_temporal_types
LD_LIBRARY_PATH=../build/release/src ./benchmark_temporal_types
```

### **3. Manual SQL Benchmark**
```bash
# Run SQL queries directly
make -f Makefile.benchmark run-sql
```

## **Expected Results**

Since TINT fails on table queries, you'll typically see:
- **TINT2 results only** in the comparison table
- **TINT operations failing** during benchmark execution
- **Performance data for TINT2** showing it's production-ready

## **Benchmark Configuration**

- **Data Size**: 100,000 rows per table
- **Iterations**: 5-10 per operation (for statistical significance)
- **Operations**: 6 core temporal operations + complex queries
- **Output**: Min/avg/max timing in milliseconds

## **Files Structure**

```
benchmark/
├── benchmark_temporal_types.cpp    # C++ benchmark
├── benchmark_sql_runner.py         # Python SQL benchmark (recommended)
├── benchmark_sql.sql              # Manual SQL benchmark
├── Makefile.benchmark             # Build and run scripts
├── BENCHMARK_README.md            # This file
└── sql_benchmark_results.json     # Results from Python benchmark
```

## **Troubleshooting**

### **TINT Operations Failing**
This is expected behavior. TINT (pointer-based) has known issues with table persistence. Use TINT2 for production.

### **Extension Loading Issues**
Ensure MobilityDuck is built and the extension path is correct:
```bash
# Check if extension exists
ls ../build/release/extension/mobilityduck/mobilityduck
```

### **Permission Issues**
Make sure the benchmark scripts are executable:
```bash
chmod +x benchmark_sql_runner.py
```

## **Conclusion**

**Use TINT2 for production applications.** The string-based approach is more robust, reliable, and suitable for real-world usage. TINT serves as a reference implementation but has fundamental limitations with database persistence. 