#pragma once

#include <type_traits>
#include "meos_wrapper_simple.hpp"
#include "tydef.hpp"

// #include "temporal/type_inout.h"

#define TIMESTAMP_ADAPT_GAP_MS (30LL * 365LL + 7LL) * 24LL * 60LL * 60LL * 1000000LL
#define OUT_DEFAULT_DECIMAL_DIGITS 15
#define WKB_EXTENDED 0x04
// #define Float8GetDatum(X) ((Datum) *(uint64_t *) &(X))
#define Int32GetDatum(X) ((Datum) (X))

namespace duckdb {

struct DataHelpers {
    template <typename T>
    static ::Datum getDatum(T arg) {
        if constexpr (std::is_same<T, int32_t>::value) {
            return Int32GetDatum(arg);
        } else if constexpr (std::is_same<T, double>::value) {
            return Float8GetDatum(arg);
        } else if constexpr (std::is_same<T, string_t>::value) {

        } else {
            static_assert(!std::is_same<T, T>::value, "Unsupported type for getDatum");
            return ::Datum();
        }
        return ::Datum();
    }

    template <typename T>
    static meosType getMeosType(::Datum arg) {
        if constexpr (std::is_same<T, int32_t>::value) {
            return T_INT4;
        } else if constexpr (std::is_same<T, double>::value) {
            return T_FLOAT8;
        } else {
            static_assert(!std::is_same<T, T>::value, "Unsupported type for getMeosType");
        }
    }

    static void spanset_tbox_slice(::Datum d, TBox *box) {
        SpanSet *ss = reinterpret_cast<SpanSet *>(d);
        if (numspan_type((meosType)ss->span.spantype)) {
            numspan_set_tbox(&ss->span, box);
        } else {
            tstzspan_set_tbox(&ss->span, box);
        }
        free(ss);
    }

    static TBox * span_tbox(const Span *s) {
        TBox *result = (TBox *)malloc(sizeof(TBox));
        if (numspan_type((meosType)s->spantype)) {
            numspan_set_tbox(s, result);
        } else { // s->spantype == T_TSTZSPAN
            tstzspan_set_tbox(s, result);
        }
        return result;
    }

    // static TBox * Tbox_from_hexwkb(string_t hexwkb_text) {
    //     const char *hexwkb = hexwkb_text.GetDataUnsafe();
    //     TBox *result = tbox_from_hexwkb(hexwkb);
    //     return result;
    // }

    // static string_t Tbox_as_text(TBox *box) {
    //     int dbl_dig_for_wkt = OUT_DEFAULT_DECIMAL_DIGITS;
    //     char *str = tbox_out(box, dbl_dig_for_wkt);
    //     string_t result = string_t(str, strlen(str));
    //     free(str);
    //     return result;
    // }

    // static string_t Tbox_as_wkb(Datum value, meosType type, bool extended) {
    //     uint8_t variant = 0;
    //     if (extended) {
    //         variant |= (uint8_t) WKB_EXTENDED;
    //     }
    //     size_t wkb_size;
    //     uint8_t *wkb = datum_as_wkb(value, type, variant, &wkb_size);
    //     string_t result = string_t((const char *)wkb, wkb_size);
    //     free(wkb);
    //     return result;
    // }

    // static string_t Tbox_as_hexwkb(Datum value, meosType type) {
    //     uint8_t variant = 0;
    //     size_t hexwkb_size;
    //     char *hexwkb = datum_as_hexwkb(value, type, variant, &hexwkb_size);
    //     string_t result = string_t(hexwkb, hexwkb_size);
    //     free(hexwkb);
    //     return result;
    // }
};

} // namespace duckdb