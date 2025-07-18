#ifndef MEOS_H
#define MEOS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <inttypes.h>

#include "meos_catalog.h"

/*****************************************************************************
 * Validity macros
 *****************************************************************************/

/**
 * @brief Macro for ensuring that a pointer is not null
 */
 #if MEOS
 #define VALIDATE_NOT_NULL(ptr, ret) \
   do { if (! ensure_not_null((void *) (ptr))) return (ret); } while (0)
#else
 #define VALIDATE_NOT_NULL(ptr, ret) \
   do { assert(ptr); } while (0)
#endif /* MEOS */

/*****************************************************************************/

/**
* @brief Macro for ensuring that a set is an integer set
*/
#if MEOS
 #define VALIDATE_INTSET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_set_isof_type((set), T_INTSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_INTSET(set, ret) \
   do { \
     assert(set); \
     assert((set)->settype == T_INTSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a set is a big integer set
*/
#if MEOS
 #define VALIDATE_BIGINTSET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_set_isof_type((set), T_BIGINTSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_BIGINTSET(set, ret) \
   do { \
     assert(set); \
     assert((set)->settype == T_BIGINTSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a set is a float set
*/
#if MEOS
 #define VALIDATE_FLOATSET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_set_isof_type((set), T_FLOATSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_FLOATSET(set, ret) \
   do { \
     assert(set); \
     assert((set)->settype == T_FLOATSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a set is a text set
*/
#if MEOS
 #define VALIDATE_TEXTSET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_set_isof_type((set), T_TEXTSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TEXTSET(set, ret) \
   do { \
     assert(set); \
     assert((set)->settype == T_TEXTSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a set is a date set
*/
#if MEOS
 #define VALIDATE_DATESET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_set_isof_type((set), T_DATESET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_DATESET(set, ret) \
   do { \
     assert(set); \
     assert((set)->settype == T_DATESET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a set is a timestamptz set
*/
#if MEOS
 #define VALIDATE_TSTZSET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_set_isof_type((set), T_TSTZSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TSTZSET(set, ret) \
   do { \
     assert(set); \
     assert((set)->settype == T_TSTZSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the span is a number span
*/
#if MEOS
 #define VALIDATE_NUMSET(set, ret) \
   do { \
         if (! ensure_not_null((void *) (set)) || \
             ! ensure_numset_type((set)->settype) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_NUMSET(set, ret) \
   do { \
     assert(set); \
     assert(numset_type((set)->settype)); \
   } while (0)
#endif /* MEOS */

/*****************************************************************************/

/**
* @brief Macro for ensuring that a span is an integer span
*/
#if MEOS
 #define VALIDATE_INTSPAN(span, ret) \
   do { \
         if (! ensure_not_null((void *) (span)) || \
             ! ensure_span_isof_type((span), T_INTSPAN) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_INTSPAN(span, ret) \
   do { \
     assert(span); \
     assert((span)->spantype == T_INTSPAN); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a span is a big integer span
*/
#if MEOS
 #define VALIDATE_BIGINTSPAN(span, ret) \
   do { \
         if (! ensure_not_null((void *) (span)) || \
             ! ensure_span_isof_type((span), T_BIGINTSPAN) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_BIGINTSPAN(span, ret) \
   do { \
     assert(span); \
     assert((span)->spantype == T_BIGINTSPAN); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a span is a float span
*/
#if MEOS
 #define VALIDATE_FLOATSPAN(span, ret) \
   do { \
         if (! ensure_not_null((void *) (span)) || \
             ! ensure_span_isof_type((span), T_FLOATSPAN) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_FLOATSPAN(span, ret) \
   do { \
     assert(span); \
     assert((span)->spantype == T_FLOATSPAN); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a span is a date span
*/
#if MEOS
 #define VALIDATE_DATESPAN(span, ret) \
   do { \
         if (! ensure_not_null((void *) (span)) || \
             ! ensure_span_isof_type((span), T_DATESPAN) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_DATESPAN(span, ret) \
   do { \
     assert(span); \
     assert((span)->spantype == T_DATESPAN); \
   } while (0)
#endif /* MEOS */


/**
* @brief Macro for ensuring that the span is a timestamptz span
*/
#if MEOS
 #define VALIDATE_TSTZSPAN(span, ret) \
   do { \
         if (! ensure_not_null((void *) (span)) || \
             ! ensure_span_isof_type((span), T_TSTZSPAN) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TSTZSPAN(span, ret) \
   do { \
     assert(span); \
     assert((span)->spantype == T_TSTZSPAN); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the span is a number span
*/
#if MEOS
 #define VALIDATE_NUMSPAN(span, ret) \
   do { \
         if (! ensure_not_null((void *) (span)) || \
             ! ensure_numspan_type((span)->spantype) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_NUMSPAN(span, ret) \
   do { \
     assert(span); \
     assert(numspan_type((span)->spantype)); \
   } while (0)
#endif /* MEOS */

/*****************************************************************************/

/**
* @brief Macro for ensuring that a span set is an integer span set
*/
#if MEOS
 #define VALIDATE_INTSPANSET(ss, ret) \
   do { \
         if (! ensure_not_null((void *) (ss)) || \
             ! ensure_spanset_isof_type((ss), T_INTSPANSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_INTSPANSET(ss, ret) \
   do { \
     assert(ss); \
     assert((ss)->spansettype == T_INTSPANSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a span set is a big integer span set
*/
#if MEOS
 #define VALIDATE_BIGINTSPANSET(ss, ret) \
   do { \
         if (! ensure_not_null((void *) (ss)) || \
             ! ensure_spanset_isof_type((ss), T_BIGINTSPANSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_BIGINTSPANSET(ss, ret) \
   do { \
     assert(ss); \
     assert((ss)->spansettype == T_BIGINTSPANSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a span set is a float span set
*/
#if MEOS
 #define VALIDATE_FLOATSPANSET(ss, ret) \
   do { \
         if (! ensure_not_null((void *) (ss)) || \
             ! ensure_spanset_isof_type((ss), T_FLOATSPANSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_FLOATSPANSET(ss, ret) \
   do { \
     assert(ss); \
     assert((ss)->spansettype == T_FLOATSPANSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that a span set is a date span set
*/
#if MEOS
 #define VALIDATE_DATESPANSET(ss, ret) \
   do { \
         if (! ensure_not_null((void *) (ss)) || \
             ! ensure_spanset_isof_type((ss), T_DATESPANSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_DATESPANSET(ss, ret) \
   do { \
     assert(ss); \
     assert((ss)->spansettype == T_DATESPANSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the span set is a timestamptz span set
*/
#if MEOS
 #define VALIDATE_TSTZSPANSET(ss, ret) \
   do { \
         if (! ensure_not_null((void *) (ss)) || \
             ! ensure_spanset_isof_type(ss, T_TSTZSPANSET) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TSTZSPANSET(ss, ret) \
   do { \
     assert(ss); \
     assert((ss)->spansettype == T_TSTZSPANSET); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the span set is a number span set
*/
#if MEOS
 #define VALIDATE_NUMSPANSET(ss, ret) \
   do { \
         if (! ensure_not_null((void *) (ss)) || \
             ! ensure_numspanset_type((ss)->spansettype) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_NUMSPANSET(ss, ret) \
   do { \
     assert(ss); \
     assert(numspanset_type((ss)->spansettype)); \
   } while (0)
#endif /* MEOS */

/*****************************************************************************/

/**
* @brief Macro for ensuring that the temporal value is a temporal Boolean
* @note The macro works for the Temporal type and its subtypes TInstant,
* TSequence, and TSequenceSet
*/
#if MEOS
 #define VALIDATE_TBOOL(temp, ret) \
   do { \
         if (! ensure_not_null((void *) (temp)) || \
             ! ensure_temporal_isof_type((Temporal *) (temp), T_TBOOL) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TBOOL(temp, ret) \
   do { \
     assert(temp); \
     assert(((Temporal *) (temp))->temptype == T_TBOOL); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the temporal value is a temporal integer
* @note The macro works for the Temporal type and its subtypes TInstant,
* TSequence, and TSequenceSet
*/
#if MEOS
 #define VALIDATE_TINT(temp, ret) \
   do { \
         if (! ensure_not_null((void *) (temp)) || \
             ! ensure_temporal_isof_type((Temporal *) (temp), T_TINT) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TINT(temp, ret) \
   do { \
     assert(temp); \
     assert(((Temporal *) (temp))->temptype == T_TINT); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the temporal value is a temporal float
* @note The macro works for the Temporal type and its subtypes TInstant,
* TSequence, and TSequenceSet
*/
#if MEOS
 #define VALIDATE_TFLOAT(temp, ret) \
   do { \
         if (! ensure_not_null((void *) (temp)) || \
             ! ensure_temporal_isof_type((Temporal *) (temp), T_TFLOAT) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TFLOAT(temp, ret) \
   do { \
     assert(temp); \
     assert(((Temporal *) (temp))->temptype == T_TFLOAT); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the temporal value is a temporal text
* @note The macro works for the Temporal type and its subtypes TInstant,
* TSequence, and TSequenceSet
*/
#if MEOS
 #define VALIDATE_TTEXT(temp, ret) \
   do { \
         if (! ensure_not_null((void *) (temp)) || \
             ! ensure_temporal_isof_type((Temporal *) (temp), T_TTEXT) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TTEXT(temp, ret) \
   do { \
     assert(temp); \
     assert(((Temporal *) (temp))->temptype == T_TTEXT); \
   } while (0)
#endif /* MEOS */

/**
* @brief Macro for ensuring that the temporal value is a temporal number
* @note The macro works for the Temporal type and its subtypes TInstant,
* TSequence, and TSequenceSet
*/
#if MEOS
 #define VALIDATE_TNUMBER(temp, ret) \
   do { \
         if (! ensure_not_null((void *) (temp)) || \
             ! ensure_tnumber_type(((Temporal *) (temp))->temptype) ) \
          return (ret); \
   } while (0)
#else
 #define VALIDATE_TNUMBER(temp, ret) \
   do { \
     assert(temp); \
     assert(tnumber_type(((Temporal *) (temp))->temptype)); \
   } while (0)
#endif /* MEOS */

/*****************************************************************************
* Macros for manipulating the 'flags' element where the less significant
* bits are MGTZXIICB, where
*   M: (GEOM) the reference geometry is stored
*   G: coordinates are geodetic
*   T: has T coordinate,
*   Z: has Z coordinate
*   X: has value or X coordinate
*   II: interpolation, whose values are
*   - 00: INTERP_NONE (undetermined) for TInstant
*   - 01: DISCRETE
*   - 10: STEP
*   - 11: LINEAR
*   C: continuous base type / Ordered set
*   B: base type passed by value
* Notice that the interpolation flags are only needed for sequence and
* sequence set subtypes.
*****************************************************************************/

/* The following flag is only used for Set and TInstant */
#define MEOS_FLAG_BYVAL      0x0001  // 1
/* The following flag is only used for Set */
#define MEOS_FLAG_ORDERED    0x0002  // 2
/* The following flag is only used for Temporal */
#define MEOS_FLAG_CONTINUOUS 0x0002  // 2
/* The following two interpolation flags are only used for TSequence and TSequenceSet */
#define MEOS_FLAGS_INTERP    0x000C  // 4 / 8
/* The following two flags are used for both bounding boxes and temporal types */
#define MEOS_FLAG_X          0x0010  // 16
#define MEOS_FLAG_Z          0x0020  // 32
#define MEOS_FLAG_T          0x0040  // 64
#define MEOS_FLAG_GEODETIC   0x0080  // 128
#define MEOS_FLAG_GEOM       0x0100  // 256

#define MEOS_FLAGS_GET_BYVAL(flags)      ((bool) (((flags) & MEOS_FLAG_BYVAL)))
#define MEOS_FLAGS_GET_ORDERED(flags)    ((bool) (((flags) & MEOS_FLAG_ORDERED)>>1))
#define MEOS_FLAGS_GET_CONTINUOUS(flags) ((bool) (((flags) & MEOS_FLAG_CONTINUOUS)>>1))
#define MEOS_FLAGS_GET_X(flags)          ((bool) (((flags) & MEOS_FLAG_X)>>4))
#define MEOS_FLAGS_GET_Z(flags)          ((bool) (((flags) & MEOS_FLAG_Z)>>5))
#define MEOS_FLAGS_GET_T(flags)          ((bool) (((flags) & MEOS_FLAG_T)>>6))
#define MEOS_FLAGS_GET_GEODETIC(flags)   ((bool) (((flags) & MEOS_FLAG_GEODETIC)>>7))
#define MEOS_FLAGS_GET_GEOM(flags)       ((bool) (((flags) & MEOS_FLAG_GEOM)>>8))

#define MEOS_FLAGS_BYREF(flags)          ((bool) (((flags) & ! MEOS_FLAG_BYVAL)))

#define MEOS_FLAGS_SET_BYVAL(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_BYVAL) : ((flags) & ~MEOS_FLAG_BYVAL))
#define MEOS_FLAGS_SET_ORDERED(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_ORDERED) : ((flags) & ~MEOS_FLAG_ORDERED))
#define MEOS_FLAGS_SET_CONTINUOUS(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_CONTINUOUS) : ((flags) & ~MEOS_FLAG_CONTINUOUS))
#define MEOS_FLAGS_SET_X(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_X) : ((flags) & ~MEOS_FLAG_X))
#define MEOS_FLAGS_SET_Z(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_Z) : ((flags) & ~MEOS_FLAG_Z))
#define MEOS_FLAGS_SET_T(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_T) : ((flags) & ~MEOS_FLAG_T))
#define MEOS_FLAGS_SET_GEODETIC(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_GEODETIC) : ((flags) & ~MEOS_FLAG_GEODETIC))
#define MEOS_FLAGS_SET_GEOM(flags, value) \
 ((flags) = (value) ? ((flags) | MEOS_FLAG_GEOM) : ((flags) & ~MEOS_FLAG_GEOM))

#define MEOS_FLAGS_GET_INTERP(flags) (((flags) & MEOS_FLAGS_INTERP) >> 2)
#define MEOS_FLAGS_SET_INTERP(flags, value) ((flags) = (((flags) & ~MEOS_FLAGS_INTERP) | ((value & 0x0003) << 2)))

#define MEOS_FLAGS_DISCRETE_INTERP(flags)   ((bool) (MEOS_FLAGS_GET_INTERP((flags)) == DISCRETE))
#define MEOS_FLAGS_STEP_INTERP(flags)       ((bool) (MEOS_FLAGS_GET_INTERP((flags)) == STEP))
#define MEOS_FLAGS_LINEAR_INTERP(flags)     ((bool) (MEOS_FLAGS_GET_INTERP((flags)) == LINEAR))
#define MEOS_FLAGS_STEP_LINEAR_INTERP(flags)  \
 ((bool) (MEOS_FLAGS_GET_INTERP((flags)) == STEP || MEOS_FLAGS_GET_INTERP((flags)) == LINEAR))

/*****************************************************************************
*  Macros for speeding up access to component values
*****************************************************************************/

/* Macros for speeding up access to component values of sets and span sets */

#if DEBUG_BUILD
extern void *SET_BBOX_PTR(const Set *s);
extern size_t *SET_OFFSETS_PTR(const Set *s);
extern Datum SET_VAL_N(const Set *s, int index);
extern const Span *SPANSET_SP_N(const SpanSet *ss, int index);
#else
/**
* @brief Return a pointer to the bounding box of a set (if any)
*/
#define SET_BBOX_PTR(s) ( (void *)( \
 ((char *) (s)) + DOUBLE_PAD(sizeof(Set)) ) )

/**
* @brief Return a pointer to the offsets array of a set
*/
#define SET_OFFSETS_PTR(s) ( (size_t *)( \
 ((char *) (s)) + DOUBLE_PAD(sizeof(Set)) + DOUBLE_PAD((s)->bboxsize) ) )

/**
* @brief Return the n-th value of a set
* @pre The argument @p index is less than the number of values in the set
*/
#define SET_VAL_N(s, index) ( (Datum) ( \
 MEOS_FLAGS_GET_BYVAL((s)->flags) ? (SET_OFFSETS_PTR(s))[index] : \
 PointerGetDatum( ((char *) (s)) + DOUBLE_PAD(sizeof(Set)) + \
   DOUBLE_PAD((s)->bboxsize) + (sizeof(size_t) * (s)->maxcount) + \
   (SET_OFFSETS_PTR(s))[index] ) ) )

/**
* @brief Return the n-th span of a span set.
* @pre The argument @p index is less than the number of spans in the span set
* @note This is the macro equivalent to #spanset_span_n.
* This function does not verify that the index is is in the correct bounds
*/
#define SPANSET_SP_N(ss, index) (const Span *) &((ss)->elems[(index)])
#endif

/*****************************************************************************/

/* Macros for speeding up access to components of temporal sequences (sets)*/

#if DEBUG_BUILD
extern size_t *TSEQUENCE_OFFSETS_PTR(const TSequence *seq);
extern const TInstant *TSEQUENCE_INST_N(const TSequence *seq, int index);
extern size_t *TSEQUENCESET_OFFSETS_PTR(const TSequenceSet *ss);
extern const TSequence *TSEQUENCESET_SEQ_N(const TSequenceSet *ss, int index);
#else
/**
* @brief Return a pointer to the offsets array of a temporal sequence
* @note The period component of the bbox is already declared in the struct
*/
#define TSEQUENCE_OFFSETS_PTR(seq) ( (size_t *)( \
 ((char *) &((seq)->period)) + (seq)->bboxsize ) )

/**
* @brief Return the n-th instant of a temporal sequence.
* @note The period component of the bbox is already declared in the struct
* @pre The argument @p index is less than the number of instants in the
* sequence
*/
#define TSEQUENCE_INST_N(seq, index) ( (const TInstant *)( \
 ((char *) &((seq)->period)) + (seq)->bboxsize + \
 (sizeof(size_t) * (seq)->maxcount) + (TSEQUENCE_OFFSETS_PTR(seq))[index] ) )

/**
* @brief Return a pointer to the offsets array of a temporal sequence set
* @note The period component of the bbox is already declared in the struct
*/
#define TSEQUENCESET_OFFSETS_PTR(ss) ( (size_t *)( \
 ((char *) &((ss)->period)) + (ss)->bboxsize ) )

/**
* @brief Return the n-th sequence of a temporal sequence set
* @note The period component of the bbox is already declared in the struct
* @pre The argument @p index is less than the number of sequences in the
* sequence set
*/
#define TSEQUENCESET_SEQ_N(ss, index) ( (const TSequence *)( \
 ((char *) &((ss)->period)) + (ss)->bboxsize + \
 (sizeof(size_t) * (ss)->maxcount) + (TSEQUENCESET_OFFSETS_PTR(ss))[index] ) )
#endif /* DEBUG_BUILD */

typedef char *(*outfunc)(Datum value, meosType type, int maxdd);

extern TInstant *tinstant_make(Datum value, meosType temptype, TimestampTz t);
extern Datum tinstant_value(const TInstant *inst);

extern char *tinstant_to_string(const TInstant *inst, int maxdd, outfunc value_out);
extern char *tinstant_out(const TInstant *inst, int maxdd);
extern char *temporal_out(const Temporal *temp, int maxdd);
// char *value_out_int(Datum value, int maxdd);
// char *tinstant_to_string(const TInstant *inst, int maxdd, char *(*value_out)(Datum value, int maxdd));

#ifdef __cplusplus
}
#endif

#endif // MEOS_H