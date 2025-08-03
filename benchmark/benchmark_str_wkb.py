#!/usr/bin/env python3
"""
SQL Benchmark Runner for TINT vs TINT3 Performance Comparison
Automatically runs SQL queries and measures runtimes for each operation.
"""

import subprocess
import time
import json
import statistics
from typing import List, Dict, Tuple
import sys
import os

class BenchmarkRunner:
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
                timeout=300
            )

            end_time = time.time()
            elapsed = (end_time - start_time) * 1000 # milliseconds
            
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
        }
    
    def setup_tables(self) -> str:
        """Create the benchmark tables."""
        return """
-- Create large benchmark tables for instants
DROP TABLE IF EXISTS big_tint2;
DROP TABLE IF EXISTS big_tint3;

CREATE TABLE big_tint2 (id INTEGER, tint2 TINT2);
CREATE TABLE big_tint3 (id INTEGER, tint3 TINT3);

-- Generate 1,000,000 rows of test data (instants)
INSERT INTO big_tint2 
SELECT
    range as id,
    TINT2(
        (range % 1000) + 1,  -- value: 1-1000
        '2023-01-01 12:00:00'::TIMESTAMP + (range % 86400) * INTERVAL '1' SECOND  -- timestamp: spread across a day
    ) as tint2
FROM range(1000000);

INSERT INTO big_tint3 
SELECT
    range as id,
    TINT3(
        (range % 1000) + 1,  -- value: 1-1000
        '2023-01-01 12:00:00'::TIMESTAMP + (range % 86400) * INTERVAL '1' SECOND  -- timestamp: spread across a day
    ) as tint3
FROM range(1000000);
"""

    def cleanup_tables(self) -> str:
        """Clean up benchmark tables."""
        return """
DROP TABLE IF EXISTS big_tint2;
DROP TABLE IF EXISTS big_tint3;
"""

    def run_benchmarks(self) -> Dict:
        """Run all benchmarks."""
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
            ("getValue TINT2", "SELECT COUNT(*) as count, SUM(getValue(tint2)) as sum_value FROM big_tint2"),
            ("getValue TINT3", "SELECT COUNT(*) as count, SUM(getValue(tint3)) as sum_value FROM big_tint3"),
            ("tempSubtype TINT2", "SELECT COUNT(DISTINCT tempSubtype(tint2)) as distinct_subtypes FROM big_tint2"),
            ("tempSubtype TINT3", "SELECT COUNT(DISTINCT tempSubtype(tint3)) as distinct_subtypes FROM big_tint3"),
            ("getTimestamp TINT2", "SELECT COUNT(getTimestamp(tint2)) as timestamp_count FROM big_tint2"),
            ("getTimestamp TINT3", "SELECT COUNT(getTimestamp(tint3)) as timestamp_count FROM big_tint3"),
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
        print("TEMPORAL TYPE BENCHMARK RESULTS (1,000,000 rows)")
        print("="*80)
        
        # Group results by operation type
        operations = {
            "getValue": ["getValue TINT2", "getValue TINT3"],
            "tempSubtype": ["tempSubtype TINT2", "tempSubtype TINT3"],
            "getTimestamp": ["getTimestamp TINT2", "getTimestamp TINT3"],
        }
        
        print(f"{'Operation':<15} {'TINT2 (ms)':<15} {'TINT3 (ms)':<15} {'Ratio':<10} {'Winner':<10}")
        print("-" * 80)

        for op_name, op_keys in operations.items():
            if op_keys[0] in self.results and op_keys[1] in self.results:
                tint2_avg = self.results[op_keys[0]]["avg"]
                tint3_avg = self.results[op_keys[1]]["avg"]
                ratio = tint3_avg / tint2_avg if tint2_avg > 0 else float('inf')
                winner = "TINT3" if tint3_avg < tint2_avg else "TINT2"

                print(f"{op_name:<15} {tint2_avg:<15.1f} {tint3_avg:<15.1f} {ratio:<10.2f} {winner:<10}")
        
        print("\n" + "="*80)
        print("DETAILED RESULTS")
        print("="*80)
        
        for name, result in self.results.items():
            print(f"\n{name}:")
            print(f"  Min: {result['min']:.1f}ms, Max: {result['max']:.1f}ms, Avg: {result['avg']:.1f}ms")
    
    def save_results(self, filename: str = "benchmark_results.json"):
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
    
    runner = BenchmarkRunner(duckdb_path)
    results = runner.run_benchmarks()
    
    if results:
        runner.print_results()
        runner.save_results()
    else:
        print("Benchmark failed to produce results.")

if __name__ == "__main__":
    main()