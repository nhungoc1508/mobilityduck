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
* @brief Create a cache of metadata information about MEOS types in global
* constant arrays
*/

#include "meos_catalog.h"
#include "type_util.h"

/* C */
#include <assert.h>
#include <limits.h>
/* MEOS */
#include <meos.h>

/*****************************************************************************
 * Global constants
 *****************************************************************************/

/**
 * @brief Global constant array containing the type names corresponding to the
 * enumeration meosType defined in file `meos_catalog.h`
 */
static const char *MEOS_TYPE_NAMES[] =
{
  [T_UNKNOWN] = "unknown",
  [T_BOOL] = "bool",
  [T_DATE] = "date",
  [T_DATEMULTIRANGE] = "datemultirange",
  [T_DATERANGE] = "daterange",
  [T_DATESET] = "dateset",
  [T_DATESPAN] = "datespan",
  [T_DATESPANSET] = "datespanset",
  [T_DOUBLE2] = "double2",
  [T_DOUBLE3] = "double3",
  [T_DOUBLE4] = "double4",
  [T_FLOAT8] = "float8",
  [T_FLOATSET] = "floatset",
  [T_FLOATSPAN] = "floatspan",
  [T_FLOATSPANSET] = "floatspanset",
  [T_INT4] = "int4",
  [T_INT4MULTIRANGE] = "int4multirange",
  [T_INT4RANGE] = "int4range",
  [T_INTSET] = "intset",
  [T_INTSPAN] = "intspan",
  [T_INTSPANSET] = "intspanset",
  [T_INT8] = "int8",
  [T_INT8MULTIRANGE] = "int8multirange",
  [T_INT8RANGE] = "int8range",
  [T_BIGINTSET] = "bigintset",
  [T_BIGINTSPAN] = "bigintspan",
  [T_BIGINTSPANSET] = "bigintspanset",
  [T_STBOX] = "stbox",
  [T_TBOOL] = "tbool",
  [T_TBOX] = "tbox",
  [T_TDOUBLE2] = "tdouble2",
  [T_TDOUBLE3] = "tdouble3",
  [T_TDOUBLE4] = "tdouble4",
  [T_TEXT] = "text",
  [T_TEXTSET] = "textset",
  [T_TFLOAT] = "tfloat",
  [T_TIMESTAMPTZ] = "timestamptz",
  [T_TINT] = "tint",
  [T_TSTZMULTIRANGE] = "tstzmultirange",
  [T_TSTZRANGE] = "tstzrange",
  [T_TSTZSET] = "tstzset",
  [T_TSTZSPAN] = "tstzspan",
  [T_TSTZSPANSET] = "tstzspanset",
  [T_TTEXT] = "ttext",
  [T_GEOMETRY] = "geometry",
  [T_GEOMSET] = "geomset",
  [T_GEOGRAPHY] = "geography",
  [T_GEOGSET] = "geogset",
  [T_TGEOMPOINT] = "tgeompoint",
  [T_TGEOGPOINT] = "tgeogpoint",
  [T_NPOINT] = "npoint",
  [T_NPOINTSET] = "npointset",
  [T_NSEGMENT] = "nsegment",
  [T_TNPOINT] = "tnpoint",
  [T_POSE] = "pose",
  [T_POSESET] = "poseset",
  [T_TPOSE] = "tpose",
  [T_CBUFFER] = "cbuffer",
  [T_CBUFFERSET] = "cbufferset",
  [T_TCBUFFER] = "tcbuffer",
  [T_TGEOMETRY] = "tgeometry",
  [T_TGEOGRAPHY] = "tgeography",
  [T_TRGEOMETRY] = "trgeometry",
};

/**
 * @brief Global constant array containing the operator names corresponding to
 * the enumeration meosOper defined in file `meos_catalog.h`
 */
static const char *MEOS_OPER_NAMES[] =
{
  [UNKNOWN_OP] = "",
  [EQ_OP] = "=",
  [NE_OP] = "<>",
  [LT_OP] = "<",
  [LE_OP] = "<=",
  [GT_OP] = ">",
  [GE_OP] = ">=",
  [ADJACENT_OP] = "-|-",
  [UNION_OP] = "+",
  [MINUS_OP] = "-",
  [INTERSECT_OP] = "*",
  [OVERLAPS_OP] = "&&",
  [CONTAINS_OP] = "@>",
  [CONTAINED_OP] = "<@",
  [SAME_OP] = "~=",
  [LEFT_OP] = "<<",
  [OVERLEFT_OP] = "&<",
  [RIGHT_OP] = ">>",
  [OVERRIGHT_OP] = "&>",
  [BELOW_OP] = "<<|",
  [OVERBELOW_OP] = "&<|",
  [ABOVE_OP] = "|>>",
  [OVERABOVE_OP] = "|&>",
  [FRONT_OP] = "<</",
  [OVERFRONT_OP] = "&</",
  [BACK_OP] = "/>>",
  [OVERBACK_OP] = "/&>",
  [BEFORE_OP] = "<<#",
  [OVERBEFORE_OP] = "&<#",
  [AFTER_OP] = "#>>",
  [OVERAFTER_OP] = "#&>",
  [EVEREQ_OP] = "?=",
  [EVERNE_OP] = "?<>",
  [EVERLT_OP] = "?<",
  [EVERLE_OP] = "?<=",
  [EVERGT_OP] = "?>",
  [EVERGE_OP] = "?>=",
  [ALWAYSEQ_OP] = "%=",
  [ALWAYSNE_OP] = "%<>",
  [ALWAYSLT_OP] = "%<",
  [ALWAYSLE_OP] = "%<=",
  [ALWAYSGT_OP] = "%>",
  [ALWAYSGE_OP] = "%>=",
};

#define TEMPSUBTYPE_STR_MAXLEN 12

/**
 * @brief Global constant array storing the string representation of the
 * concrete subtypes of temporal types
 */
static const char *MEOS_TEMPSUBTYPE_NAMES[] =
{
  [ANYTEMPSUBTYPE] = "Any subtype",
  [TINSTANT] = "Instant",
  [TSEQUENCE] = "Sequence",
  [TSEQUENCESET] = "SequenceSet"
};

#define INTERP_STR_MAXLEN 8

/**
 * @brief Global constant array containing the interpolation names
 * corresponding to the enumeration interpType defined in file `meos_catalog.h`
 * @note The names are in lowercase since they are used in error messages
 */
static const char * MEOS_INTERPTYPE_NAMES[] =
{
  [INTERP_NONE] = "None",
  [DISCRETE] = "Discrete",
  [STEP] = "Step",
  [LINEAR] = "Linear"
};

/*****************************************************************************/

/**
 * @brief Global constant array that keeps type information for the defined set
 * types
 */
static const settype_catalog_struct MEOS_SETTYPE_CATALOG[] =
{
  /* settype        basetype */
  {T_INTSET,        T_INT4},
  {T_BIGINTSET,     T_INT8},
  {T_FLOATSET,      T_FLOAT8},
  {T_DATESET,       T_DATE},
  {T_TSTZSET,       T_TIMESTAMPTZ},
  {T_TEXTSET,       T_TEXT},
  {T_GEOMSET,       T_GEOMETRY},
  {T_GEOGSET,       T_GEOGRAPHY},
  {T_POSESET,       T_POSE},
  {T_NPOINTSET,     T_NPOINT},
  {T_CBUFFERSET,    T_CBUFFER},
};

/**
 * @brief Global constant array that keeps type information for the defined
 * span types
 */
static const spantype_catalog_struct MEOS_SPANTYPE_CATALOG[] =
{
  /* spantype       basetype */
  {T_INTSPAN,       T_INT4},
  {T_BIGINTSPAN,    T_INT8},
  {T_FLOATSPAN,     T_FLOAT8},
  {T_DATESPAN,      T_DATE},
  {T_TSTZSPAN,      T_TIMESTAMPTZ},
};

/**
 * @brief Global constant array that keeps type information for the defined
 * span set types
 */
static const spansettype_catalog_struct MEOS_SPANSETTYPE_CATALOG[] =
{
  /* spansettype    spantype */
  {T_INTSPANSET,    T_INTSPAN},
  {T_BIGINTSPANSET, T_BIGINTSPAN},
  {T_FLOATSPANSET,  T_FLOATSPAN},
  {T_DATESPANSET,   T_DATESPAN},
  {T_TSTZSPANSET,   T_TSTZSPAN},
};

/**
 * @brief Global constant array that keeps type information for the defined
 * temporal types
 */
static const temptype_catalog_struct MEOS_TEMPTYPE_CATALOG[] =
{
  /* temptype    basetype */
  {T_TDOUBLE2,   T_DOUBLE2},
  {T_TDOUBLE3,   T_DOUBLE3},
  {T_TDOUBLE4,   T_DOUBLE4},
  {T_TBOOL,      T_BOOL},
  {T_TINT,       T_INT4},
  {T_TFLOAT,     T_FLOAT8},
  {T_TTEXT,      T_TEXT},
  {T_TGEOMPOINT, T_GEOMETRY},
  {T_TGEOGPOINT, T_GEOGRAPHY},
  {T_TGEOMETRY,  T_GEOMETRY},
  {T_TGEOGRAPHY, T_GEOGRAPHY},
  {T_TPOSE,      T_POSE},
  {T_TRGEOMETRY, T_POSE},
  {T_TNPOINT,    T_NPOINT},
  {T_TCBUFFER,   T_CBUFFER},
};

/*****************************************************************************/

/**
 * @brief Return true if a type is a spatiotemporal type
 * @details This function is used for features common to all spatiotemporal
 * types, in particular, all of them use the same bounding box @ STBox.
 * Therefore, it is used for the indexes and selectivity functions.
 */
bool tspatial_type(meosType type) {
    if (type == T_TGEOMPOINT || type == T_TGEOGPOINT || 
        type == T_TGEOMETRY || type == T_TGEOGRAPHY
    #if CBUFFER
        || type == T_TCBUFFER
    #endif
    #if NPOINT
        || type == T_TNPOINT
    #endif
    #if POSE
        || type == T_TPOSE
    #endif
    #if RGEO
        || type == T_TRGEOMETRY
    #endif
        )
        return true;
    return false;
}

/*****************************************************************************
 * Cache functions
 *****************************************************************************/

/**
 * @brief Return the base type from the temporal type
 */
meosType temptype_basetype(meosType type) {
    int n = sizeof(MEOS_TEMPTYPE_CATALOG) / sizeof(temptype_catalog_struct);
    for (int i = 0; i < n; i++)
    {
        if (MEOS_TEMPTYPE_CATALOG[i].temptype == type)
        return MEOS_TEMPTYPE_CATALOG[i].basetype;
    }
    /* We only arrive here on error */
    // meos_error(ERROR, MEOS_ERR_INTERNAL_TYPE_ERROR,
    //     "type %d is not a temporal type", type);
    // return T_UNKNOWN;
}

/**
 * @brief Return true if the type is a base type of one of the template types,
 * that is, @p Set, @p Span, @p SpanSet, and @p Temporal
 * @note This function is only used in the asserts
 */
bool meos_basetype(meosType type) {
    if (type == T_BOOL || type == T_INT4 || type == T_INT8 || type == T_FLOAT8 ||
        type == T_TEXT || type == T_DATE || type == T_TIMESTAMPTZ ||
        /* The doubleX are internal types used for temporal aggregation */
        type == T_DOUBLE2 || type == T_DOUBLE3 || type == T_DOUBLE4 ||
        type == T_GEOMETRY || type == T_GEOGRAPHY
    #if CBUFFER
        || type == T_CBUFFER
    #endif
    #if NPOINT
        || type == T_NPOINT
    #endif
    #if POSE || RGEO
        || type == T_POSE
    #endif
        )
        return true;
    return false;
}

/**
 * @brief Return true if the values of the base type are passed by value
 */
bool basetype_byvalue(meosType type) {
    // assert(meos_basetype(type));
    // if (type == T_BOOL || type == T_INT4 || type == T_INT8 || type == T_FLOAT8 ||
    //     type == T_DATE || type == T_TIMESTAMPTZ)
    //     return true;
    // return false;
    // TODO: fix
    return true;
}

/**
 * @brief Return true if the type is a temporal continuous type
 */
bool temptype_continuous(meosType type) {
    if (type == T_TFLOAT || type == T_TDOUBLE2 || type == T_TDOUBLE3 ||
        type == T_TDOUBLE4 || type == T_TGEOMPOINT || type == T_TGEOGPOINT
    #if CBUFFER
        || type == T_TCBUFFER
    #endif
    #if NPOINT
        || type == T_TNPOINT
    #endif
    #if POSE
        || type == T_TPOSE
    #endif
    #if RGEO
        || type == T_TRGEOMETRY
    #endif
        )
        return true;
    return false;
}

/**
 * @brief Return the length of a base type
 * @return On error return SHRT_MAX
 */
int16 basetype_length(meosType type) {
    if (basetype_byvalue(type))
        return sizeof(Datum);
    if (type == T_DOUBLE2)
        return sizeof(double2);
    if (type == T_DOUBLE3)
        return sizeof(double3);
    if (type == T_DOUBLE4)
        return sizeof(double4);
    if (type == T_TEXT)
        return -1;
    if (type == T_GEOMETRY || type == T_GEOGRAPHY)
        return -1;
    #if CBUFFER
    if (type == T_CBUFFER)
        return -1;
    #endif
    #if NPOINT
    if (type == T_NPOINT)
        return sizeof(Npoint);
    #endif
    #if POSE || RGEO
    if (type == T_POSE)
        return -1;
    #endif
    // meos_error(ERROR, MEOS_ERR_INTERNAL_TYPE_ERROR,
    //     "Unknown base type: %s", meostype_name(type));
    return SHRT_MAX;
}

/**
 * @brief Return the string representation of a base value
 * @return On error return @p NULL
 */
char *basetype_out(Datum value, meosType type, int maxdd) {
    // assert(meos_basetype(type)); assert(maxdd >= 0);

    switch (type)
    {
        case T_TIMESTAMPTZ:
        return pg_timestamptz_out(DatumGetTimestampTz(value));
        case T_DATE:
        return pg_date_out(DatumGetTimestampTz(value));
        case T_BOOL:
        return bool_out(DatumGetBool(value));
        case T_INT4:
        return int4_out(DatumGetInt32(value));
        case T_INT8:
        return int8_out(DatumGetInt64(value));
        case T_FLOAT8:
        return float8_out(DatumGetFloat8(value), maxdd);
        case T_TEXT:
        return text_out(DatumGetTextP(value));
// #if DEBUG_BUILD
//     case T_DOUBLE2:
//         return double2_out(DatumGetDouble2P(value), maxdd);
//     case T_DOUBLE3:
//         return double3_out(DatumGetDouble3P(value), maxdd);
//     case T_DOUBLE4:
//         return double4_out(DatumGetDouble4P(value), maxdd);
// #endif
//     case T_GEOMETRY:
//     case T_GEOGRAPHY:
//         /* Hex-encoded ASCII Well-Known Binary (HexWKB) representation */
//         return geo_out(DatumGetGserializedP(value));
// #if CBUFFER
//     case T_CBUFFER:
//        return cbuffer_out(DatumGetCbufferP(value), maxdd);
// #endif
// #if NPOINT
//     case T_NPOINT:
//        return npoint_out(DatumGetNpointP(value), maxdd);
// #endif
// #if POSE || RGEO
//     case T_POSE:
//        return pose_out(DatumGetPoseP(value), maxdd);
// #endif
    default: /* Error! */
    // meos_error(ERROR, MEOS_ERR_INTERNAL_TYPE_ERROR,
    //      "Unknown output function for type: %s", meostype_name(type));
        return NULL;
   }
}