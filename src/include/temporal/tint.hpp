#pragma once

#include "common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

extern "C" {
    #include <postgres.h>
    #include <utils/timestamp.h>
    #include <meos.h>
    #include <meos_internal.h>
}

namespace duckdb {

class ExtensionLoader;

struct TInt {
    uintptr_t meos_ptr;

    TInt() : meos_ptr(0) {}
    TInt(uintptr_t meos_ptr) : meos_ptr(meos_ptr) {}
    TInt(const TInt &other) : meos_ptr(other.meos_ptr) {}
    TInt(TInt &&other) noexcept : meos_ptr(other.meos_ptr) { other.meos_ptr = 0; }
    ~TInt() {}

    TInt &operator=(const TInt &other) {
        if (this != &other) {
            meos_ptr = other.meos_ptr;
        }
        return *this;
    }
    TInt &operator=(TInt &&other) noexcept {
        if (this != &other) {
            meos_ptr = other.meos_ptr;
            other.meos_ptr = 0;
        }
        return *this;
    }

    bool operator==(const TInt &other) const {
        if (meos_ptr == 0 && other.meos_ptr == 0) return true;
        if (meos_ptr == 0 || other.meos_ptr == 0) return false;
        
        Temporal *temp1 = GetTemporal();
        Temporal *temp2 = other.GetTemporal();
        return temporal_eq(temp1, temp2);
    }
    bool operator!=(const TInt &other) const {
        return !(*this == other);
    }

    void Serialize(Serializer &serializer) const;
    void Deserialize(Deserializer &deserializer);

    Temporal* GetTemporal() const;
    tempSubtype GetSubtype() const;
    meosType GetTemptype() const;
    bool IsNull() const;

    static TInt FromTemporal(Temporal* temp);
    static TInt FromString(const string &str);
    static string ToString(const TInt &tint);

    static LogicalType TIntMake();
    static void RegisterType(DatabaseInstance &instance);
    static void RegisterCastFunctions(DatabaseInstance &instance);
    static void RegisterScalarFunctions(DatabaseInstance &instance);
};

struct TIntHash {
    size_t operator()(const TInt &tint) const;
};

} // namespace duckdb