#ifndef MEOS_H
#define MEOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <inttypes.h>

typedef struct
{ 
    int32_t vl_len_;
    uint8_t temptype;
    uint8_t subtype;
    int16_t flags;
} Temporal;

extern Temporal *tpoint_cumulative_length(const Temporal *temp);

// --- Minimal TInstant port for DuckDB integration ---
// For now, use int64_t for TimestampTz and uintptr_t for Datum
typedef int64_t TimestampTz;
typedef uintptr_t Datum;

typedef struct {
    int32_t vl_len_;
    uint8_t temptype;
    uint8_t subtype;
    int16_t flags;
    Datum value;
    TimestampTz t;
} TInstant;

TInstant *tinstant_make(Datum value, uint8_t temptype, TimestampTz t);
Datum tinstant_value(const TInstant *inst);
char *value_out_int(Datum value, int maxdd);
char *tinstant_to_string(const TInstant *inst, int maxdd, char *(*value_out)(Datum value, int maxdd));

#ifdef __cplusplus
}
#endif

#endif // MEOS_H