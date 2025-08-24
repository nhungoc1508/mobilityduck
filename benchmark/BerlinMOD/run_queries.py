#!/usr/bin/env python3
"""
Query runner for BerlinMOD benchmark
"""

import subprocess
import time
import json
import sys
import os
from typing import Dict, Tuple

QUERIES_NUM = 17

class QueryRunner:
    def __init__(self, duckdb_path: str = "../../build/release/duckdb",
                       queries_path: str = "./sql/queries",
                       output_path: str = "./results/output"):
        self.duckdb_path = duckdb_path
        self.queries_path = queries_path
        self.output_path = output_path
        self.queries_num = QUERIES_NUM

    def run_sql(self, filename: str) -> Tuple[float, int]:
        success = False
        print(f"\nRunning {filename}")
        start_time = time.time()
        sql_path = os.path.join(self.queries_path, filename)
        if not os.path.exists(sql_path):
            print(f"\tError: Query file not found: {sql_path}")
            return -1, -1
        
        with open(sql_path, "r") as f:
            sql = f.read()

        while not success:
            result = subprocess.run(
                [self.duckdb_path, "./databases/berlinmod.db"],
                input=sql,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                success = True
            else:
                print(f"\tError running query: {result.stderr}")
                print("\tTrying again...")
                time.sleep(1)
                start_time = time.time()

        end_time = time.time()
        elapsed = (end_time - start_time) * 1000 # milliseconds

        print(f"\tDone in {elapsed:.2f}ms")

        line_count = self.run_validation(filename)
        print(f"\tOutput row count: {line_count}")
        
        return elapsed, line_count

    def run_validation(self, filename: str) -> int:
        output_file = f"{self.output_path}/{filename.replace('.sql', '.csv')}"
        if not os.path.exists(output_file):
            print(f"\tError: Output file not found: {output_file}")
            return -1
        with open(output_file, "r") as f:
            line_count = sum(1 for _ in f)
        if line_count > 0:
            line_count -= 1
        return line_count

    def run_queries(self) -> Dict:
        results = dict()

        for query_num in range(1, self.queries_num + 1):
            filename = f"query_{query_num}.sql"
            elapsed, line_count = self.run_sql(filename)
            if elapsed != -1:
                results[filename] = {
                    "elapsed": elapsed,
                    "row_count": line_count
                }
        
        return results

def main():
    duckdb_path = "../../build/release/duckdb"
    if not os.path.exists(duckdb_path):
        print(f"Error: DuckDB executable not found at {duckdb_path}")
        print("Please make sure you're running this from the benchmark directory and DuckDB is built.")
        sys.exit(1)
    
    runner = QueryRunner(duckdb_path)
    results = runner.run_queries()
    
    with open("./results/stats/run_queries.json", "w") as f:
        json.dump(results, f, indent=4)
    
    print(f"\nResults saved to ./results/stats/run_queries.json")

if __name__ == "__main__":
    main()