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
 * @brief Functions for building a cache of temporal types and operators.
 */

 #ifndef __MEOS_CATALOG_H__
 #define __MEOS_CATALOG_H__
 
 /* C */
 #include <stdbool.h>
 /* PostgreSQL */
 #ifndef int16
 typedef signed short int16;
 #endif
 /* MEOS */
 #include <meos.h>
 
 /*****************************************************************************
  * Data structures
  *****************************************************************************/
 
 /**
  * @brief Enumeration that defines the built-in and temporal types used in
  * MobilityDB.
  */
 typedef enum
 {
   T_UNKNOWN        = 0,   /**< unknown type */
   T_BOOL           = 1,   /**< boolean type */
   T_DATE           = 2,   /**< date type */
   T_DATEMULTIRANGE = 3,   /**< PostgreSQL date multirange type */
   T_DATERANGE      = 4,   /**< PostgreSQL date range type */
   T_DATESET        = 5,   /**< date set type */
   T_DATESPAN       = 6,   /**< date span type */
   T_DATESPANSET    = 7,   /**< date span set type */
   T_DOUBLE2        = 8,   /**< double2 type */
   T_DOUBLE3        = 9,   /**< double3 type */
   T_DOUBLE4        = 10,   /**< double4 type */
   T_FLOAT8         = 11,  /**< float8 type */
   T_FLOATSET       = 12,  /**< float8 set type */
   T_FLOATSPAN      = 13,  /**< float8 span type */
   T_FLOATSPANSET   = 14,  /**< float8 span set type */
   T_INT4           = 15,  /**< int4 type */
   T_INT4MULTIRANGE = 16,  /**< PostgreSQL int4 multirange type */
   T_INT4RANGE      = 17,  /**< PostgreSQL int4 range type */
   T_INTSET         = 18,  /**< int4 set type */
   T_INTSPAN        = 19,  /**< int4 span type */
   T_INTSPANSET     = 20,  /**< int4 span set type */
   T_INT8           = 21,  /**< int8 type */
   T_INT8MULTIRANGE = 52,  /**< PostgreSQL int8 multirange type */
   T_INT8RANGE      = 53,  /**< PostgreSQL int8 range type */
   T_BIGINTSET      = 22,  /**< int8 set type */
   T_BIGINTSPAN     = 23,  /**< int8 span type */
   T_BIGINTSPANSET  = 24,  /**< int8 span set type */
   T_STBOX          = 25,  /**< spatiotemporal box type */
   T_TBOOL          = 26,  /**< temporal boolean type */
   T_TBOX           = 27,  /**< temporal box type */
   T_TDOUBLE2       = 28,  /**< temporal double2 type */
   T_TDOUBLE3       = 29,  /**< temporal double3 type */
   T_TDOUBLE4       = 30,  /**< temporal double4 type */
   T_TEXT           = 31,  /**< text type */
   T_TEXTSET        = 32,  /**< text type */
   T_TFLOAT         = 33,  /**< temporal float type */
   T_TIMESTAMPTZ    = 34,  /**< timestamp with time zone type */
   T_TINT           = 35,  /**< temporal integer type */
   T_TSTZMULTIRANGE = 36,  /**< PostgreSQL timestamp with time zone multirange type */
   T_TSTZRANGE      = 37,  /**< PostgreSQL timestamp with time zone range type */
   T_TSTZSET        = 38,  /**< timestamptz set type */
   T_TSTZSPAN       = 39,  /**< timestamptz span type */
   T_TSTZSPANSET    = 40,  /**< timestamptz span set type */
   T_TTEXT          = 41,  /**< temporal text type */
   T_GEOMETRY       = 42,  /**< geometry type */
   T_GEOMSET        = 43,  /**< geometry set type */
   T_GEOGRAPHY      = 44,  /**< geography type */
   T_GEOGSET        = 45,  /**< geography set type */
   T_TGEOMPOINT     = 46,  /**< temporal geometry point type */
   T_TGEOGPOINT     = 47,  /**< temporal geography point type */
   T_NPOINT         = 48,  /**< network point type */
   T_NPOINTSET      = 49,  /**< network point set type */
   T_NSEGMENT       = 50,  /**< network segment type */
   T_TNPOINT        = 51,  /**< temporal network point type */
   T_POSE           = 54,  /**< pose type */
   T_POSESET        = 55,  /**< pose set type */
   T_TPOSE          = 56,  /**< temporal pose type */
   T_CBUFFER        = 57,  /**< buffer type */
   T_CBUFFERSET     = 58,  /**< buffer set type */
   T_TCBUFFER       = 59,  /**< temporal buffer type */
   T_TGEOMETRY      = 60,  /**< temporal geometry type */
   T_TGEOGRAPHY     = 61,  /**< temporal geography type */
   T_TRGEOMETRY     = 62,  /**< temporal rigid geometry type */
 } meosType;
 
 #define NO_MEOS_TYPES 63
 
 /**
  * Enumeration that defines the classes of Boolean operators used in
  * MobilityDB.
  */
 typedef enum
 {
   UNKNOWN_OP      = 0,
   EQ_OP           = 1,  /**< Equality `=` operator */
   NE_OP           = 2,  /**< Distinct `!=` operator */
   LT_OP           = 3,  /**< Less than `<` operator */
   LE_OP           = 4,  /**< Less than or equal to `<=` operator */
   GT_OP           = 5,  /**< Greater than `<` operator */
   GE_OP           = 6,  /**< Greater than or equal to `>=` operator */
   ADJACENT_OP     = 7,  /**< Adjacent `-|-` operator */
   UNION_OP        = 8,  /**< Union `+` operator */
   MINUS_OP        = 9,  /**< Minus `-` operator */
   INTERSECT_OP    = 10, /**< Intersection `*` operator */
   OVERLAPS_OP     = 11, /**< Overlaps `&&` operator */
   CONTAINS_OP     = 12, /**< Contains `@>` operator */
   CONTAINED_OP    = 13, /**< Contained `<@` operator */
   SAME_OP         = 14, /**< Same `~=` operator */
   LEFT_OP         = 15, /**< Left `<<` operator */
   OVERLEFT_OP     = 16, /**< Overleft `&<` operator */
   RIGHT_OP        = 17, /**< Right `>>` operator */
   OVERRIGHT_OP    = 18, /**< Overright `&>` operator */
   BELOW_OP        = 19, /**< Below `<<|` operator */
   OVERBELOW_OP    = 20, /**< Overbelow `&<|` operator */
   ABOVE_OP        = 21, /**< Above `|>>` operator */
   OVERABOVE_OP    = 22, /**< Overbove `|&>` operator */
   FRONT_OP        = 23, /**< Front `<</` operator */
   OVERFRONT_OP    = 24, /**< Overfront `&</` operator */
   BACK_OP         = 25, /**< Back `/>>` operator */
   OVERBACK_OP     = 26, /**< Overback `/&>` operator */
   BEFORE_OP       = 27, /**< Before `<<#` operator */
   OVERBEFORE_OP   = 28, /**< Overbefore `&<#` operator */
   AFTER_OP        = 29, /**< After `#>>` operator */
   OVERAFTER_OP    = 30, /**< Overafter `#&>` operator */
   EVEREQ_OP       = 31, /**< Evereq `?=` operator */
   EVERNE_OP       = 32, /**< Everne `?<>` operator */
   EVERLT_OP       = 33, /**< Everlt `?<` operator */
   EVERLE_OP       = 34, /**< Everle `?<=` operator */
   EVERGT_OP       = 35, /**< Evergt `?>` operator */
   EVERGE_OP       = 36, /**< Everge `?>=` operator */
   ALWAYSEQ_OP     = 37, /**< Alwayseq `%=` operator */
   ALWAYSNE_OP     = 38, /**< Alwaysne `%<>` operator */
   ALWAYSLT_OP     = 39, /**< Alwayslt `%<` operator */
   ALWAYSLE_OP     = 40, /**< Alwaysle `%<=` operator */
   ALWAYSGT_OP     = 41, /**< Alwaysgt `%>` operator */
   ALWAYSGE_OP     = 42, /**< Alwaysge `%>=` operator */
 } meosOper;

/**
 * Structure to represent the temporal type cache array.
 */
 typedef struct
 {
   meosType temptype;    /**< Enum value of the temporal type */
   meosType basetype;    /**< Enum value of the base type */
 } temptype_catalog_struct;
 
 /**
  * Structure to represent the span type cache array.
  */
 typedef struct
 {
   meosType settype;     /**< Enum value of the set type */
   meosType basetype;    /**< Enum value of the base type */
 } settype_catalog_struct;
 
 /**
  * Structure to represent the span type cache array.
  */
 typedef struct
 {
   meosType spantype;    /**< Enum value of the span type */
   meosType basetype;    /**< Enum value of the base type */
 } spantype_catalog_struct;
 
 /**
  * Structure to represent the spanset type cache array.
  */
 typedef struct
 {
   meosType spansettype;    /**< Enum value of the span type */
   meosType spantype;       /**< Enum value of the base type */
 } spansettype_catalog_struct;
 
/*****************************************************************************/

 /**
 * Structure to represent values of the internal type for computing aggregates
 * for temporal number types
 */
typedef struct
{
  double a;
  double b;
} double2;

/**
 * Structure to represent values of the internal type for computing aggregates
 * for 2D temporal points
 */
typedef struct
{
  double a;
  double b;
  double c;
} double3;

/**
 * Structure to represent values of the internal type for computing aggregates
 * for 3D temporal points
 */
typedef struct
{
  double a;
  double b;
  double c;
  double d;
} double4;

 extern meosType temptype_basetype(meosType type);
 extern bool meos_basetype(meosType type);
 extern bool basetype_byvalue(meosType type);
 extern bool temptype_continuous(meosType type);
 extern int16 basetype_length(meosType type);

 extern char *basetype_out(Datum value, meosType type, int maxdd);
 
 #endif /* __MEOS_CATALOG_H__ */ 