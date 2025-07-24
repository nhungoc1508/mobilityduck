#include "meos_internal.h"
#include "type_util.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

/**
 * @ingroup meos_internal_temporal_constructor
 * @brief Return a temporal instant from the arguments
 * @details The memory structure of a temporal instant is as follows
 * @code
 * ----------------
 * ( TInstant )_X |
 * ----------------
 * @endcode
 * where the attribute `value` of type `Datum` in the struct stores the base
 * value independently of whether it is passed by value or by reference.
 * For values passed by reference, the additional bytes needed are appended at
 * the end of the struct upon creation.
 *
 * @param[in] value Value
 * @param[in] temptype Temporal type
 * @param[in] t Timestamp
 */

TInstant *tinstant_make(Datum value, meosType temptype, TimestampTz t) {
    /* Ensure validity of arguments */
    // int32_t tspatial_srid;
    // if (tspatial_type(temptype) && temptype != T_TNPOINT)
    // {
    //   meosType basetype = temptype_basetype(temptype);
    //   tspatial_srid = spatial_srid(value, basetype);
    //   /* Ensure that the SRID is geodetic for geography */
    //   if (tgeodetic_type(temptype) && tspatial_srid != SRID_UNKNOWN && 
    //       ! ensure_srid_is_latlong(tspatial_srid))
    //     return NULL;
    //   /* Ensure that a geometry/geography is not empty */
    //   if (tgeo_type_all(temptype) && 
    //       ! ensure_not_empty(DatumGetGserializedP(value)))
    //     return NULL;
    // }
  
    size_t value_offset = sizeof(TInstant) - sizeof(Datum);
    size_t size = value_offset;
    /* Create the temporal instant */
    size_t value_size;
    void *value_from;
    meosType basetype = temptype_basetype(temptype);
    bool typbyval = basetype_byvalue(basetype);
    /* Copy value */
    if (typbyval)
    {
        /* For base types passed by value */
        value_size = DOUBLE_PAD(sizeof(Datum));
        value_from = &value;
    }
    else
    {
        /* For base types passed by reference */
        int16 typlen = basetype_length(basetype);
        // value_from = DatumGetPointer(value);
        value_from = (void *)(uintptr_t)value;
        value_size = (typlen != -1) ? DOUBLE_PAD((unsigned int) typlen) :
        DOUBLE_PAD(*(const uint32_t *)value_from);
    }
    size += value_size;
    // TInstant *result = palloc0(size);
    TInstant *result = (TInstant *)calloc(1, size);
    void *value_to = ((char *) result) + value_offset;
    memcpy(value_to, value_from, value_size);
    /* Initialize fixed-size values */
    result->temptype = temptype;
    result->subtype = TINSTANT;
    result->t = t;
    // SET_VARSIZE(result, size);
    result->vl_len_ = size;
    MEOS_FLAGS_SET_BYVAL(result->flags, typbyval);
    MEOS_FLAGS_SET_CONTINUOUS(result->flags, temptype_continuous(temptype));
    MEOS_FLAGS_SET_X(result->flags, true);
    MEOS_FLAGS_SET_T(result->flags, true);
    // // TODO Should we bypass the tests on tnpoint ?
    // if (tspatial_type(temptype) && temptype != T_TNPOINT)
    // {
    //   int16 flags = spatial_flags(value, basetype);
    //   MEOS_FLAGS_SET_Z(result->flags, MEOS_FLAGS_GET_Z(flags));
    //   MEOS_FLAGS_SET_GEODETIC(result->flags, MEOS_FLAGS_GET_GEODETIC(flags));
    // }
    return result;
}

/**
 * @ingroup meos_internal_temporal_accessor
 * @brief Return (a pointer to) the base value of a temporal instant
 * @param[in] inst Temporal instant
 * @csqlfn #Tinstant_value()
 */
Datum tinstant_value_p(const TInstant *inst) {
    // assert(inst);
    /* For base types passed by value */
    if (MEOS_FLAGS_GET_BYVAL(inst->flags))
        return inst->value;
    /* For base types passed by reference */
    return (Datum)(uintptr_t)&inst->value;
}

/**
 * @ingroup meos_internal_temporal_accessor
 * @brief Return a copy of the base value of a temporal instant
 * @param[in] inst Temporal instant
 */
Datum tinstant_value(const TInstant *inst) {
    // assert(inst);
    return datum_copy(tinstant_value_p(inst), temptype_basetype(inst->temptype));
}

/**
 * @brief Return the Well-Known Text (WKT) representation of a temporal instant
 * @param[in] inst Temporal instant
 * @param[in] maxdd Maximum number of decimal digits
 * @param[in] value_out Function called to output the base value depending on
 * its type
 */
char *tinstant_to_string(const TInstant *inst, int maxdd, outfunc value_out) {
    assert(inst); assert(maxdd >= 0);
    char *t = pg_timestamptz_out(inst->t);
    meosType basetype = temptype_basetype(inst->temptype);
    char *value = value_out(tinstant_value_p(inst), basetype, maxdd);
    size_t size = strlen(value) + strlen(t) + 2;
    // char *result = palloc(size);
    char *result = (char *)malloc(size);
    snprintf(result, size, "%s@%s", value, t);
    // pfree(t); pfree(value);
    free(t); free(value);
    return result;
}

/**
 * @ingroup meos_internal_temporal_inout
 * @brief Return the Well-Known Text (WKT) representation of a temporal instant
 * @param[in] inst Temporal instant
 * @param[in] maxdd Maximum number of decimal digits
 */
inline char * tinstant_out(const TInstant *inst, int maxdd) {
    return tinstant_to_string(inst, maxdd, &basetype_out);
}

/**
 * @ingroup meos_internal_temporal_inout
 * @brief Return the Well-Known Text (WKT) representation of a temporal value
 * @param[in] temp Temporal value
 * @param[in] maxdd Maximum number of decimal digits
 */
char * temporal_out(const Temporal *temp, int maxdd) {
    /* Ensure the validity of the arguments */
    // VALIDATE_NOT_NULL(temp, NULL);
    // if (! ensure_not_negative(maxdd))
    //     return NULL;

    // assert(temptype_subtype(temp->subtype));
    // TODO: handle cases
    switch (temp->subtype)
    {
    case TINSTANT:
        return tinstant_out((TInstant *) temp, maxdd);
    // case TSEQUENCE:
        // return tsequence_out((TSequence *) temp, maxdd);
    default: /* TSEQUENCESET */
        // return tsequenceset_out((TSequenceSet *) temp, maxdd);
        return NULL;
    }
}

// Example value_out function for integers
// char *value_out_int(Datum value, int maxdd) {
//     (void)maxdd;
//     char *buf = (char *)malloc(32);
//     snprintf(buf, 32, "%ld", (long)value);
//     return buf;
// }

// char *tinstant_to_string(const TInstant *inst, int maxdd, char *(*value_out)(Datum value, int maxdd)) {
//     if (!inst || !value_out) return NULL;
//     char *valstr = value_out(inst->value, maxdd);
//     char *buf = (char *)malloc(128 + strlen(valstr));
//     snprintf(buf, 128 + strlen(valstr), "%s@%" PRId64, valstr, inst->t);
//     free(valstr);
//     return buf;
// }