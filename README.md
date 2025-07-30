# MobilityDuck

This repository is based on https://github.com/duckdb/extension-template.

---

MobilityDuck is a binding for DuckDB built on top of [MEOS (Mobility Engine, Open Source)](https://libmeos.org/), a C library that enables the manipulation of temporal and spatiotemporal data based on [MobilityDB](https://mobilitydb.com/)'s data types and functions.

## Requirements
```sh
apt install build-essential cmake libgeos-dev libproj-dev libjson-c-dev libgsl-dev
```

## Building MobilityDuck
### Clone the repository
```sh
git clone --recurse-submodules https://github.com/nhungoc1508/mobilityduck.git
```
Note that `--recurse-submodules` will ensure DuckDB is pulled which is required to build the extension.

### Build steps
To build the extension, from the root directory (`mobilityduck`), run (for first build):
```sh
make
```

For subsequent builds, use `ninja` for faster build relying on cache:
```sh
GEN=ninja make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/mobilityduck/mobilityduck.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `mobilityduck.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. Some examples:
```
D SELECT '100@2025-01-01 10:00:00+05'::TINT as tint;
┌────────────────────────────┐
│            tint            │
│            tint            │
├────────────────────────────┤
│ 100@2025-01-01 05:00:00+00 │
└────────────────────────────┘

D SELECT duration('{1@2000-01-01, 2@2000-01-02, 1@2000-01-03}'::TINT, true) as duration;
┌──────────┐
│ duration │
│ interval │
├──────────┤
│ 2 days   │
└──────────┘

D SELECT tstzspan('[2000-01-01,2000-01-01]') as ts_span;
┌──────────────────────────────────────────────────┐
│                     ts_span                      │
│                       span                       │
├──────────────────────────────────────────────────┤
│ [2000-01-01 00:00:00+00, 2000-01-02 00:00:00+00) │
└──────────────────────────────────────────────────┘

D SELECT timeSpan(tgeompoint('{Point(1 1)@2000-01-01, Point(2 2)@2000-01-02, Point(1 1)@2000-01-03}')) as span;
┌──────────────────────────────────────────────────┐
│                       span                       │
│                       span                       │
├──────────────────────────────────────────────────┤
│ [2000-01-01 00:00:00+00, 2000-01-03 00:00:00+00] │
└──────────────────────────────────────────────────┘

D SELECT * FROM setUnnest(textset('{"highway", "car", "bike"}'));
┌─────────┐
│ unnest  │
│ varchar │
├─────────┤
│ bike    │
│ car     │
│ highway │
└─────────┘
```

## Running the tests
Test files are located in `./test/sql`. These SQL tests can be run using:
```sh
make test
```