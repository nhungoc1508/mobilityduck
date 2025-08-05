#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <random>
#include <iomanip>
#include "duckdb.hpp"

using namespace duckdb;

// Benchmark configuration
const int NUM_ITERATIONS = 10;
const int NUM_ROWS = 100000;

// Test data generation
std::vector<std::string> generate_test_data(int count) {
    std::vector<std::string> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> value_dist(1, 1000);
    std::uniform_int_distribution<> year_dist(2020, 2025);
    std::uniform_int_distribution<> month_dist(1, 12);
    std::uniform_int_distribution<> day_dist(1, 28);
    std::uniform_int_distribution<> hour_dist(0, 23);
    std::uniform_int_distribution<> minute_dist(0, 59);
    
    for (int i = 0; i < count; i++) {
        int value = value_dist(gen);
        int year = year_dist(gen);
        int month = month_dist(gen);
        int day = day_dist(gen);
        int hour = hour_dist(gen);
        int minute = minute_dist(gen);
        
        std::string timestamp = std::to_string(year) + "-" + 
                               std::string(month < 10 ? "0" : "") + std::to_string(month) + "-" +
                               std::string(day < 10 ? "0" : "") + std::to_string(day) + " " +
                               std::string(hour < 10 ? "0" : "") + std::to_string(hour) + ":" +
                               std::string(minute < 10 ? "0" : "") + std::to_string(minute) + ":00+00";
        
        data.push_back(std::to_string(value) + "@" + timestamp);
    }
    return data;
}

// Benchmark timing utility
class Timer {
private:
    std::chrono::high_resolution_clock::time_point start_time;
    std::string name;
    
public:
    Timer(const std::string& n) : name(n) {
        start_time = std::chrono::high_resolution_clock::now();
    }
    
    ~Timer() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        // std::cout << name << ": " << duration.count() << " microseconds" << std::endl;
    }
    
    double elapsed() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        return duration.count();
    }
};

// Benchmark results structure
struct BenchmarkResult {
    std::string operation;
    double tint_time;
    double tint2_time;
    double speedup;
    
    BenchmarkResult(const std::string& op, double tint, double tint2) 
        : operation(op), tint_time(tint), tint2_time(tint2) {
        speedup = tint2_time / tint_time;
    }
};

// Run a single benchmark
double run_benchmark(Connection& conn, const std::string& query, int iterations) {
    double total_time = 0.0;
    
    for (int i = 0; i < iterations; i++) {
        Timer timer("Query");
        auto result = conn.Query(query);
        if (!result->HasError()) {
            // Query succeeded
        } else {
            std::cerr << "Query failed: " << result->GetError() << std::endl;
            return -1.0;
        }
        total_time += timer.elapsed();
    }
    
    return total_time / iterations;
}

// Benchmark constructor operations
BenchmarkResult benchmark_constructor(Connection& conn) {
    std::cout << "\n=== Benchmarking Constructor Operations ===" << std::endl;
    
    // TINT constructor (pointer-based)
    std::string tint_query = "SELECT TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 constructor (string-based)
    std::string tint2_query = "SELECT TINT2(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint2";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Constructor", tint_time, tint2_time);
}

// Benchmark cast operations
BenchmarkResult benchmark_cast(Connection& conn) {
    std::cout << "\n=== Benchmarking Cast Operations ===" << std::endl;
    
    // TINT cast
    std::string tint_query = "SELECT '42@2023-01-01 12:00:00+00'::TINT as tint";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 cast
    std::string tint2_query = "SELECT '42@2023-01-01 12:00:00+00'::TINT2 as tint2";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Cast", tint_time, tint2_time);
}

// Benchmark value extraction operations
BenchmarkResult benchmark_value_extraction(Connection& conn) {
    std::cout << "\n=== Benchmarking Value Extraction Operations ===" << std::endl;
    
    // TINT value extraction
    std::string tint_query = "SELECT getValue(TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as value";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 value extraction
    std::string tint2_query = "SELECT getValue(TINT2(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as value";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Value Extraction", tint_time, tint2_time);
}

// Benchmark subtype operations
BenchmarkResult benchmark_subtype(Connection& conn) {
    std::cout << "\n=== Benchmarking Subtype Operations ===" << std::endl;
    
    // TINT subtype
    std::string tint_query = "SELECT tempSubtype(TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as subtype";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 subtype
    std::string tint2_query = "SELECT tempSubtype(TINT2(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as subtype";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Subtype", tint_time, tint2_time);
}

// Benchmark timestamp operations
BenchmarkResult benchmark_timestamp(Connection& conn) {
    std::cout << "\n=== Benchmarking Timestamp Operations ===" << std::endl;
    
    // TINT timestamp
    std::string tint_query = "SELECT getTimestamp(TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as timestamp";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 timestamp
    std::string tint2_query = "SELECT getTimestamp(TINT2(42, '2023-01-01 12:00:00'::TIMESTAMPTZ)) as timestamp";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Timestamp", tint_time, tint2_time);
}

// Benchmark sequence operations
BenchmarkResult benchmark_sequences(Connection& conn) {
    std::cout << "\n=== Benchmarking Sequence Operations ===" << std::endl;
    
    // TINT sequences
    std::string tint_query = "SELECT sequences('[1@2025-01-01, 2@2025-01-02, 1@2025-01-03]'::TINT) as sequences";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 sequences
    std::string tint2_query = "SELECT sequences('[1@2025-01-01, 2@2025-01-02, 1@2025-01-03]'::TINT2) as sequences";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Sequences", tint_time, tint2_time);
}

// Benchmark complex operations
BenchmarkResult benchmark_complex_operations(Connection& conn) {
    std::cout << "\n=== Benchmarking Complex Operations ===" << std::endl;
    
    // TINT complex operations
    std::string tint_query = "SELECT getValue(tint), tempSubtype(tint), getTimestamp(tint) FROM (SELECT TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint) t";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS);
    
    // TINT2 complex operations
    std::string tint2_query = "SELECT getValue(tint2), tempSubtype(tint2), getTimestamp(tint2) FROM (SELECT TINT2(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint2) t";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS);
    
    return BenchmarkResult("Complex Operations", tint_time, tint2_time);
}

// Benchmark bulk operations
BenchmarkResult benchmark_bulk_operations(Connection& conn) {
    std::cout << "\n=== Benchmarking Bulk Operations ===" << std::endl;
    
    // Create temporary tables with test data
    conn.Query("CREATE TABLE test_tint AS SELECT TINT(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint");
    conn.Query("CREATE TABLE test_tint2 AS SELECT TINT2(42, '2023-01-01 12:00:00'::TIMESTAMPTZ) as tint2");
    
    // TINT bulk operations
    std::string tint_query = "SELECT getValue(tint), tempSubtype(tint), getTimestamp(tint) FROM test_tint LIMIT 1000";
    double tint_time = run_benchmark(conn, tint_query, NUM_ITERATIONS / 10);
    
    // TINT2 bulk operations
    std::string tint2_query = "SELECT getValue(tint2), tempSubtype(tint2), getTimestamp(tint2) FROM test_tint2 LIMIT 1000";
    double tint2_time = run_benchmark(conn, tint2_query, NUM_ITERATIONS / 10);
    
    // Clean up
    conn.Query("DROP TABLE test_tint");
    conn.Query("DROP TABLE test_tint2");
    
    return BenchmarkResult("Bulk Operations", tint_time, tint2_time);
}

// Helper: Bulk insert TINT and TINT2 tables
void create_large_tables(Connection& conn) {
    std::cout << "\nCreating large tables (" << NUM_ROWS << " rows each)..." << std::endl;
    conn.Query("DROP TABLE IF EXISTS big_tint");
    conn.Query("DROP TABLE IF EXISTS big_tint2");
    conn.Query("CREATE TABLE big_tint (id INTEGER, tint TINT)");
    conn.Query("CREATE TABLE big_tint2 (id INTEGER, tint2 TINT2)");
    auto data = generate_test_data(NUM_ROWS);
    // Batched inserts for speed
    int batch = 1000;
    for (int i = 0; i < NUM_ROWS; i += batch) {
        std::string sql = "INSERT INTO big_tint VALUES ";
        std::string sql2 = "INSERT INTO big_tint2 VALUES ";
        for (int j = 0; j < batch && (i + j) < NUM_ROWS; ++j) {
            if (j > 0) { sql += ", "; sql2 += ", "; }
            sql += "(" + std::to_string(i + j) + ", TINT(" + data[i + j] + "))";
            sql2 += "(" + std::to_string(i + j) + ", TINT2('" + data[i + j] + "'))";
        }
        conn.Query(sql);
        conn.Query(sql2);
        if ((i/batch)%10 == 0) std::cout << "." << std::flush;
    }
    std::cout << " done!\n";
}

// Helper: Run and time a query multiple times, return min/avg/max
struct TimingStats {
    double min = 1e12, max = 0, sum = 0;
    int n = 0;
    void add(double t) { min = std::min(min, t); max = std::max(max, t); sum += t; n++; }
    double avg() const { return sum / n; }
};

TimingStats run_bulk_benchmark(Connection& conn, const std::string& query, int iterations) {
    TimingStats stats;
    for (int i = 0; i < iterations; ++i) {
        Timer timer("BulkQuery");
        auto result = conn.Query(query);
        if (result->HasError()) {
            std::cerr << "Query failed: " << result->GetError() << std::endl;
            continue;
        }
        stats.add(timer.elapsed());
    }
    return stats;
}

void print_bulk_stats(const std::string& op, const TimingStats& t, const TimingStats& t2) {
    std::cout << std::setw(20) << op
              << " | TINT min/avg/max: " << std::setw(8) << (int)t.min << "/" << (int)t.avg() << "/" << (int)t.max
              << " | TINT2 min/avg/max: " << std::setw(8) << (int)t2.min << "/" << (int)t2.avg() << "/" << (int)t2.max
              << std::endl;
}

void run_large_scale_benchmarks(Connection& conn) {
    std::cout << "\n==== LARGE-SCALE BENCHMARKS (" << NUM_ROWS << " rows) ====" << std::endl;
    create_large_tables(conn);
    int iters = NUM_ITERATIONS;
    // getValue
    auto t = run_bulk_benchmark(conn, "SELECT SUM(getValue(tint)) FROM big_tint", iters);
    auto t2 = run_bulk_benchmark(conn, "SELECT SUM(getValue(tint2)) FROM big_tint2", iters);
    print_bulk_stats("getValue", t, t2);
    // tempSubtype
    t = run_bulk_benchmark(conn, "SELECT COUNT(DISTINCT tempSubtype(tint)) FROM big_tint", iters);
    t2 = run_bulk_benchmark(conn, "SELECT COUNT(DISTINCT tempSubtype(tint2)) FROM big_tint2", iters);
    print_bulk_stats("tempSubtype", t, t2);
    // getTimestamp
    t = run_bulk_benchmark(conn, "SELECT COUNT(getTimestamp(tint)) FROM big_tint", iters);
    t2 = run_bulk_benchmark(conn, "SELECT COUNT(getTimestamp(tint2)) FROM big_tint2", iters);
    print_bulk_stats("getTimestamp", t, t2);
    // sequences
    t = run_bulk_benchmark(conn, "SELECT COUNT(sequences(tint)) FROM big_tint", iters);
    t2 = run_bulk_benchmark(conn, "SELECT COUNT(sequences(tint2)) FROM big_tint2", iters);
    print_bulk_stats("sequences", t, t2);
    // Clean up
    conn.Query("DROP TABLE big_tint");
    conn.Query("DROP TABLE big_tint2");
}

// Print benchmark results
void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "TEMPORAL TYPE BENCHMARK RESULTS: TINT vs TINT2" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    
    std::cout << std::left << std::setw(25) << "Operation" 
              << std::setw(15) << "TINT (μs)" 
              << std::setw(15) << "TINT2 (μs)" 
              << std::setw(15) << "Speedup" 
              << std::setw(10) << "Winner" << std::endl;
    std::cout << std::string(80, '-') << std::endl;
    
    double total_tint = 0.0;
    double total_tint2 = 0.0;
    
    for (const auto& result : results) {
        std::cout << std::left << std::setw(25) << result.operation
                  << std::setw(15) << std::fixed << std::setprecision(2) << result.tint_time
                  << std::setw(15) << std::fixed << std::setprecision(2) << result.tint2_time
                  << std::setw(15) << std::fixed << std::setprecision(2) << result.speedup
                  << std::setw(10) << (result.speedup > 1.0 ? "TINT" : "TINT2") << std::endl;
        
        total_tint += result.tint_time;
        total_tint2 += result.tint2_time;
    }
    
    std::cout << std::string(80, '-') << std::endl;
    std::cout << std::left << std::setw(25) << "TOTAL"
              << std::setw(15) << std::fixed << std::setprecision(2) << total_tint
              << std::setw(15) << std::fixed << std::setprecision(2) << total_tint2
              << std::setw(15) << std::fixed << std::setprecision(2) << (total_tint2 / total_tint)
              << std::setw(10) << (total_tint2 < total_tint ? "TINT2" : "TINT") << std::endl;
    
    std::cout << std::string(80, '=') << std::endl;
}

int main() {
    std::cout << "Temporal Type Benchmark: TINT vs TINT2" << std::endl;
    DuckDB db(nullptr);
    Connection conn(db);
    // Large-scale benchmark
    run_large_scale_benchmarks(conn);
    // Old micro-benchmarks (optional, comment out if not needed)
    /*
    std::vector<BenchmarkResult> results;
    results.push_back(benchmark_constructor(conn));
    results.push_back(benchmark_cast(conn));
    results.push_back(benchmark_value_extraction(conn));
    results.push_back(benchmark_subtype(conn));
    results.push_back(benchmark_timestamp(conn));
    results.push_back(benchmark_sequences(conn));
    results.push_back(benchmark_complex_operations(conn));
    results.push_back(benchmark_bulk_operations(conn));
    print_results(results);
    */
    return 0;
} 