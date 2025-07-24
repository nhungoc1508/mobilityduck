/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2016-2025, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * MobilityDB includes portions of PostGIS version 3 source code released
 * under the GNU General Public License (GPLv2 or later).
 * Copyright (c) 2001-2025, PostGIS contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

/**
 * @file
 * @brief General utility functions for temporal types
 */

#include "type_util.h"
#include <inttypes.h>

/**
 * @brief Copy a Datum if it is passed by reference
 */
Datum datum_copy(Datum value, meosType basetype) {
    /* For types passed by value */
    if (basetype_byvalue(basetype))
        return value;
    /* For types passed by reference */
    int typlen = basetype_length(basetype);
    void *src_ptr = (void *)(uintptr_t)value;
    size_t value_size = (typlen != -1)
        ? (size_t)typlen
        : ((const TInstant *)src_ptr)->vl_len_;

    void *result = malloc(value_size);
    memcpy(result, src_ptr, value_size);
    return (Datum)(uintptr_t)result;
}

char *int4_out(int32_t value) {
    char *buf = malloc(32);
    snprintf(buf, 32, "%d", value);
    return buf;
}

char *int8_out(int64_t value) {
    char *buf = malloc(32);
    snprintf(buf, 32, "%" PRId64, value);
    return buf;
}

char *float8_out(double value, int maxdd) {
    char *buf = malloc(64);
    snprintf(buf, 64, "%.*g", maxdd, value);
    return buf;
}

char *bool_out(bool value) {
    char *buf = malloc(6);
    strcpy(buf, value ? "true" : "false");
    return buf;
}

char *text_out(const char *value) {
    return strdup(value);
}

// ---------- NEW IMPLEMENTATION ----------
char *pg_timestamptz_out(TimestampTz value) {
    // Convert to seconds since PostgreSQL epoch
    printf("timestamp value: %ld\n", value);
    int64_t seconds_since_unix_epoch = value / USECS_PER_SEC;

    // Convert to struct tm in UTC
    time_t t = (time_t)seconds_since_unix_epoch;
    struct tm tm_utc;
#if defined(_WIN32) || defined(_WIN64)
    gmtime_s(&tm_utc, &t);
#else
    gmtime_r(&t, &tm_utc);
#endif

    // Format as "YYYY-MM-DD HH:MM:SS+00"
    char *buf = malloc(32);
    strftime(buf, 32, "%Y-%m-%d %H:%M:%S+00", &tm_utc);
    return buf;
}

char *pg_date_out(TimestampTz value) {
    // value: microseconds since Unix epoch
    time_t t = (time_t)(value / USECS_PER_SEC);
    struct tm tm_utc;
#if defined(_WIN32) || defined(_WIN64)
    gmtime_s(&tm_utc, &t);
#else
    gmtime_r(&t, &tm_utc);
#endif

    // Format as "YYYY-MM-DD"
    char *buf = malloc(16);
    strftime(buf, 16, "%Y-%m-%d", &tm_utc);
    return buf;
}