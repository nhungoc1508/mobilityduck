#pragma once

#include <cstddef>
#include <cstdint>
// Include MEOS headers
extern "C" {
    #include <meos.h>
    #include <meos_internal.h>
}

// Create explicit aliases for MEOS types
// Use the original MEOS type names but with explicit qualification
using MeosInterval = ::Interval;  // Explicitly use global MEOS Interval
using MeosTimestamp = ::Timestamp;  // Explicitly use global MEOS Timestamp 