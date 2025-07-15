#pragma once
#include <cstdint>

namespace duckdb {

struct TemporalGeometryProperties {
private:
    static constexpr const uint8_t Z = 0x01;
    static constexpr const uint8_t M = 0x02;
    static constexpr const uint8_t BBOX = 0x04;
    uint8_t flags = 0;

public:
    explicit TemporalGeometryProperties(uint8_t flags = 0) : flags(flags) {}
    TemporalGeometryProperties(bool has_z, bool has_m) {
        SetZ(has_z);
        SetM(has_m);
    }

    inline bool HasZ() const {
        return (flags & Z) != 0;
    }
    inline bool HasM() const {
        return (flags & M) != 0;
    }
    inline bool HasBBox() const {
        return (flags & BBOX) != 0;
    }
    inline void SetZ(bool value) {
        flags = value ? (flags | Z) : (flags & ~Z);
    }
    inline void SetM(bool value) {
        flags = value ? (flags | M) : (flags & ~M);
    }
    inline void SetBBox(bool value) {
        flags = value ? (flags | BBOX) : (flags & ~BBOX);
    }
};

} // namespace duckdb