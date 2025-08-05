#!/usr/bin/env python3
"""
SQL Benchmark Runner for TINT vs TINT2 Performance Comparison
Automatically runs SQL queries and measures runtimes for each operation.
"""

import subprocess
import time
import json
import statistics
from typing import List, Dict, Tuple
import sys
import os

class SQLBenchmarkRunner:
    def __init__(self, duckdb_path: str = "../build/release/duckdb"):
        self.duckdb_path = duckdb_path
        self.results = {}
        
    def run_query(self, query: str, description: str, iterations: int = 5) -> Dict:
        """Run a single query multiple times and return timing statistics."""
        times = []
        
        print(f"Running {description}...", end=" ", flush=True)
        
        for i in range(iterations):
            start_time = time.time()
            
            # Run the query using the persistent database
            result = subprocess.run(
                [self.duckdb_path, "benchmark.db"],
                input=query,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            end_time = time.time()
            elapsed = (end_time - start_time) * 1000  # Convert to milliseconds
            
            if result.returncode != 0:
                print(f"\nError running query: {result.stderr}")
                print(f"Query was: {query}")
                return {"error": result.stderr}
            
            times.append(elapsed)
            print(".", end="", flush=True)
        
        print(f" done! ({statistics.mean(times):.1f}ms avg)")
        
        return {
            "description": description,
            "times": times,
            "min": min(times),
            "max": max(times),
            "avg": statistics.mean(times),
            "median": statistics.median(times)
        }
    
    def setup_tables(self) -> str:
        """Create the benchmark tables."""
        return """
-- Create large benchmark tables for instants
DROP TABLE IF EXISTS big_tint;
DROP TABLE IF EXISTS big_tint2;

CREATE TABLE big_tint (id INTEGER, tint TINT);
CREATE TABLE big_tint2 (id INTEGER, tint2 TINT2);

-- Generate 100,000 rows of test data (instants)
INSERT INTO big_tint 
SELECT 
    range as id,
    TINT(
        (range % 1000) + 1,  -- value: 1-1000
        '2023-01-01 12:00:00'::TIMESTAMP + (range % 86400) * INTERVAL '1' SECOND  -- timestamp: spread across a day
    ) as tint
FROM range(100000);

INSERT INTO big_tint2 
SELECT 
    range as id,
    TINT2(
        (range % 1000) + 1,  -- value: 1-1000
        '2023-01-01 12:00:00'::TIMESTAMP + (range % 86400) * INTERVAL '1' SECOND  -- timestamp: spread across a day
    ) as tint2
FROM range(100000);

-- Create large benchmark tables for sequences
DROP TABLE IF EXISTS big_tint_seq;
DROP TABLE IF EXISTS big_tint2_seq;

CREATE TABLE big_tint_seq AS
SELECT
    range as id,
    '[1@2023-01-01, 2@2023-01-02, 3@2023-01-03]'::TINT as tint_sequence
FROM range(100000);

CREATE TABLE big_tint2_seq AS
SELECT
    range as id,
    '[1@2023-01-01, 2@2023-01-02, 3@2023-01-03]'::TINT2 as tint2_sequence
FROM range(100000);
"""
    
    def cleanup_tables(self) -> str:
        """Clean up benchmark tables."""
        return """
DROP TABLE IF EXISTS big_tint;
DROP TABLE IF EXISTS big_tint2;
DROP TABLE IF EXISTS big_tint_seq;
DROP TABLE IF EXISTS big_tint2_seq;
"""
    
    def run_benchmarks(self) -> Dict:
        """Run all benchmark queries and collect timing data."""
        print("Setting up benchmark tables...")
        
        # Remove old database file
        if os.path.exists("benchmark.db"):
            os.remove("benchmark.db")
        
        # Load extension and create tables in a single session
        setup_sql = f"""
LOAD 'build/release/extension/mobilityduck/mobilityduck';

{self.setup_tables()}
"""
        
        setup_result = subprocess.run(
            [self.duckdb_path, "benchmark.db"],
            input=setup_sql,
            capture_output=True,
            text=True
        )
        
        if setup_result.returncode != 0:
            print(f"Error setting up tables: {setup_result.stderr}")
            print(f"Setup stdout: {setup_result.stdout}")
            return {}
        
        print("Running benchmarks...")
        
        # Define benchmark queries
        benchmarks = [
            ("getValue TINT", "SELECT COUNT(*) as count, SUM(getValue(tint)) as sum_value FROM big_tint"),
            ("getValue TINT2", "SELECT COUNT(*) as count, SUM(getValue(tint2)) as sum_value FROM big_tint2"),
            ("tempSubtype TINT", "SELECT COUNT(DISTINCT tempSubtype(tint)) as distinct_subtypes FROM big_tint"),
            ("tempSubtype TINT2", "SELECT COUNT(DISTINCT tempSubtype(tint2)) as distinct_subtypes FROM big_tint2"),
            ("getTimestamp TINT", "SELECT COUNT(getTimestamp(tint)) as timestamp_count FROM big_tint"),
            ("getTimestamp TINT2", "SELECT COUNT(getTimestamp(tint2)) as timestamp_count FROM big_tint2"),
            ("sequences TINT", "SELECT COUNT(sequences(tint_sequence)) as sequences_count FROM big_tint_seq"),
            ("sequences TINT2", "SELECT COUNT(sequences(tint2_sequence)) as sequences_count FROM big_tint2_seq"),
            ("GroupBy TINT", "SELECT COUNT(*) FROM (SELECT (getValue(tint) / 100) as value_group, COUNT(*) as count FROM big_tint GROUP BY (getValue(tint) / 100))"),
            ("GroupBy TINT2", "SELECT COUNT(*) FROM (SELECT (getValue(tint2) / 100) as value_group, COUNT(*) as count FROM big_tint2 GROUP BY (getValue(tint2) / 100))"),
            ("Window TINT", "SELECT COUNT(*) FROM (SELECT id, getValue(tint) as value, ROW_NUMBER() OVER (ORDER BY getValue(tint)) as row_num FROM big_tint WHERE id < 1000)"),
            ("Window TINT2", "SELECT COUNT(*) FROM (SELECT id, getValue(tint2) as value, ROW_NUMBER() OVER (ORDER BY getValue(tint2)) as row_num FROM big_tint2 WHERE id < 1000)")
        ]
        
        # Run each benchmark in the same database session
        for name, query in benchmarks:
            result = self.run_query(query, name)
            if "error" not in result:
                self.results[name] = result
            else:
                print(f"Failed to run {name}: {result['error']}")
        
        # Cleanup
        subprocess.run(
            [self.duckdb_path, "benchmark.db"],
            input=self.cleanup_tables(),
            capture_output=True,
            text=True
        )
        
        # Remove database file
        if os.path.exists("benchmark.db"):
            os.remove("benchmark.db")
        
        return self.results
    
    def print_results(self):
        """Print a formatted comparison table."""
        if not self.results:
            print("No results to display.")
            return
        
        print("\n" + "="*80)
        print("TEMPORAL TYPE BENCHMARK RESULTS (100,000 rows)")
        print("="*80)
        
        # Group results by operation type
        operations = {
            "getValue": ["getValue TINT", "getValue TINT2"],
            "tempSubtype": ["tempSubtype TINT", "tempSubtype TINT2"],
            "getTimestamp": ["getTimestamp TINT", "getTimestamp TINT2"],
            "sequences": ["sequences TINT", "sequences TINT2"],
            "GroupBy": ["GroupBy TINT", "GroupBy TINT2"],
            "Window": ["Window TINT", "Window TINT2"]
        }
        
        print(f"{'Operation':<15} {'TINT (ms)':<15} {'TINT2 (ms)':<15} {'Ratio':<10} {'Winner':<10}")
        print("-" * 80)
        
        for op_name, op_keys in operations.items():
            if op_keys[0] in self.results and op_keys[1] in self.results:
                tint_avg = self.results[op_keys[0]]["avg"]
                tint2_avg = self.results[op_keys[1]]["avg"]
                ratio = tint2_avg / tint_avg if tint_avg > 0 else float('inf')
                winner = "TINT2" if tint2_avg < tint_avg else "TINT"
                
                print(f"{op_name:<15} {tint_avg:<15.1f} {tint2_avg:<15.1f} {ratio:<10.2f} {winner:<10}")
        
        print("\n" + "="*80)
        print("DETAILED RESULTS")
        print("="*80)
        
        for name, result in self.results.items():
            print(f"\n{name}:")
            print(f"  Min: {result['min']:.1f}ms, Max: {result['max']:.1f}ms, Avg: {result['avg']:.1f}ms, Median: {result['median']:.1f}ms")
    
    def save_results(self, filename: str = "sql_benchmark_results.json"):
        """Save results to JSON file."""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to {filename}")

def main():
    # Check if DuckDB executable exists
    duckdb_path = "../build/release/duckdb"
    if not os.path.exists(duckdb_path):
        print(f"Error: DuckDB executable not found at {duckdb_path}")
        print("Please make sure you're running this from the benchmark directory and DuckDB is built.")
        sys.exit(1)
    
    runner = SQLBenchmarkRunner(duckdb_path)
    results = runner.run_benchmarks()
    
    if results:
        runner.print_results()
        runner.save_results()
    else:
        print("Benchmark failed to produce results.")

if __name__ == "__main__":
    main() 