#!/usr/bin/env python3
"""
Debug script to test the exact benchmark setup with detailed error reporting
"""

import subprocess
import sys
import os

def test_benchmark_setup():
    """Test the exact setup from the benchmark with detailed error reporting."""
    
    duckdb_path = "../build/release/duckdb"
    if not os.path.exists(duckdb_path):
        print(f"Error: DuckDB executable not found at {duckdb_path}")
        return
    
    # Remove old database file
    if os.path.exists("debug_persistent.db"):
        os.remove("debug_persistent.db")
    
    print("Testing benchmark setup with persistent database...")
    print("=" * 60)
    
    # Step 1: Load extension and create tables
    setup_sql = """
LOAD 'build/release/extension/mobilityduck/mobilityduck';

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
    
    print("Step 1: Setting up tables...")
    setup_result = subprocess.run(
        [duckdb_path, "debug_persistent.db"],
        input=setup_sql,
        capture_output=True,
        text=True
    )
    
    if setup_result.returncode != 0:
        print(f"❌ Setup failed:")
        print(f"Error: {setup_result.stderr}")
        print(f"Output: {setup_result.stdout}")
        return
    else:
        print("✅ Setup successful")
    
    # Step 2: Test individual queries
    test_queries = [
        ("Count TINT rows", "SELECT COUNT(*) FROM big_tint;"),
        ("Count TINT2 rows", "SELECT COUNT(*) FROM big_tint2;"),
        ("TINT getValue sample", "SELECT getValue(tint) FROM big_tint LIMIT 5;"),
        ("TINT2 getValue sample", "SELECT getValue(tint2) FROM big_tint2 LIMIT 5;"),
        ("TINT getValue aggregate", "SELECT COUNT(*) as count, SUM(getValue(tint)) as sum_value FROM big_tint;"),
        ("TINT2 getValue aggregate", "SELECT COUNT(*) as count, SUM(getValue(tint2)) as sum_value FROM big_tint2;"),
    ]
    
    print("\nStep 2: Testing queries...")
    for test_name, query in test_queries:
        print(f"\n{test_name}:")
        print(f"Query: {query}")
        
        result = subprocess.run(
            [duckdb_path, "debug_persistent.db"],
            input=query,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print(f"✅ SUCCESS")
            if result.stdout.strip():
                print(f"Result:\n{result.stdout.strip()}")
        else:
            print(f"❌ FAILED")
            print(f"Error: {result.stderr.strip()}")
            print(f"Return code: {result.returncode}")
        
        print("-" * 40)
    
    # Cleanup
    # if os.path.exists("debug_persistent.db"):
    #     os.remove("debug_persistent.db")

if __name__ == "__main__":
    test_benchmark_setup() 