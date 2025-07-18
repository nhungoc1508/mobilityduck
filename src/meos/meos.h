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
 * @brief External API of the Mobility Engine Open Source (MEOS) library
 */

 #ifndef __MEOS_H__
 #define __MEOS_H__
 
 /* C */
 #include <stdbool.h>
 #include <stdint.h>
 #include <stddef.h>

  typedef char *Pointer;
  typedef uintptr_t Datum;

  typedef signed char int8;
  typedef signed short int16;
  typedef signed int int32;
  typedef long int int64;

  typedef unsigned char uint8;
  typedef unsigned short uint16;
  typedef unsigned int uint32;
  typedef unsigned long int uint64;

  typedef int32 DateADT;
  typedef int64 TimeADT;
  typedef int64 Timestamp;
  typedef int64 TimestampTz;
  typedef int64 TimeOffset;
  typedef int32 fsec_t;

  #define Int32GetDatum(X) ((Datum)(X))
  #define DatumGetInt32(X) ((int32_t)(X))
  #define Int64GetDatum(X) ((Datum)(X))
  #define DatumGetInt64(X) ((int64_t)(X))
  #define Float8GetDatum(X) ((Datum)(X))
  #define DatumGetFloat8(X) ((double)(X))
  #define TimestampTzGetDatum(X) ((Datum)(X))
  #define DatumGetTimestampTz(X) ((TimestampTz)(X))
  #define BoolGetDatum(X) ((Datum)(X))
  #define DatumGetBool(X) ((bool)(X))
  #define DatumGetTextP(X) ((char *)(X))
 
 /*****************************************************************************
  * Type definitions
  *****************************************************************************/
 
 /**
  * @brief Align to double
  */
 #define DOUBLE_PAD(size) ( (size) + ((size) % 8 ? (8 - (size) % 8) : 0 ) )
 
 /**
  * Structure to represent sets of values
  */
 typedef struct
 {
   int32 vl_len_;        /**< Varlena header (do not touch directly!) */
   uint8 settype;        /**< Set type */
   uint8 basetype;       /**< Span basetype */
   int16 flags;          /**< Flags */
   int32 count;          /**< Number of elements */
   int32 maxcount;       /**< Maximum number of elements */
   int16 bboxsize;       /**< Size of the bouding box */
 } Set;
 
 /**
  * Structure to represent spans (a.k.a. ranges)
  */
 typedef struct
 {
   uint8 spantype;       /**< span type */
   uint8 basetype;       /**< span basetype */
   bool lower_inc;       /**< lower bound is inclusive (vs exclusive) */
   bool upper_inc;       /**< upper bound is inclusive (vs exclusive) */
   char padding[4];      /**< Not used */
   Datum lower;          /**< lower bound value */
   Datum upper;          /**< upper bound value */
 } Span;
 
 /**
  * Structure to represent span sets
  */
 typedef struct
 {
   int32 vl_len_;        /**< Varlena header (do not touch directly!) */
   uint8 spansettype;    /**< Span set type */
   uint8 spantype;       /**< Span type */
   uint8 basetype;       /**< Span basetype */
   char padding;         /**< Not used */
   int32 count;          /**< Number of elements */
   int32 maxcount;       /**< Maximum number of elements */
   Span span;            /**< Bounding span */
   Span elems[1];        /**< Beginning of variable-length data */
 } SpanSet;
 
 /**
  * Structure to represent temporal boxes
  */
 typedef struct
 {
   Span period;          /**< time span */
   Span span;            /**< value span */
   int16 flags;          /**< flags */
 } TBox;
 
 /**
  * Structure to represent spatiotemporal boxes
  */
 typedef struct
 {
   Span period;          /**< time span */
   double xmin;          /**< minimum x value */
   double ymin;          /**< minimum y value */
   double zmin;          /**< minimum z value */
   double xmax;          /**< maximum x value */
   double ymax;          /**< maximum y value */
   double zmax;          /**< maximum z value */
   int32_t srid;         /**< SRID */
   int16 flags;          /**< flags */
 } STBox;
 
 /**
  * @brief Enumeration that defines the temporal subtypes used in MEOS
  */
 typedef enum
 {
   ANYTEMPSUBTYPE =   0,  /**< Any temporal subtype */
   TINSTANT =         1,  /**< Temporal instant subtype */
   TSEQUENCE =        2,  /**< Temporal sequence subtype */
   TSEQUENCESET =     3,  /**< Temporal sequence set subtype */
 } tempSubtype;
 
 /**
  * @brief Enumeration that defines the interpolation types used in MEOS
  */
 typedef enum
 {
   INTERP_NONE =    0,
   DISCRETE =       1,
   STEP =           2,
   LINEAR =         3,
 } interpType;
 
 /**
  * Structure to represent the common structure of temporal values of
  * any temporal subtype
  */
 typedef struct
 {
   int32 vl_len_;        /**< Varlena header (do not touch directly!) */
   uint8 temptype;       /**< Temporal type */
   uint8 subtype;        /**< Temporal subtype */
   int16 flags;          /**< Flags */
   /* variable-length data follows */
 } Temporal;
 
 /**
  * Structure to represent temporal values of instant subtype
  */
 typedef struct
 {
   int32 vl_len_;        /**< Varlena header (do not touch directly!) */
   uint8 temptype;       /**< Temporal type */
   uint8 subtype;        /**< Temporal subtype */
   int16 flags;          /**< Flags */
   TimestampTz t;        /**< Timestamp (8 bytes) */
   Datum value;          /**< Base value for types passed by value,
                              first 8 bytes of the base value for values
                              passed by reference. The extra bytes
                              needed are added upon creation. */
   /* variable-length data follows */
 } TInstant;
 
 /**
  * Structure to represent temporal values of sequence subtype
  */
 typedef struct
 {
   int32 vl_len_;        /**< Varlena header (do not touch directly!) */
   uint8 temptype;       /**< Temporal type */
   uint8 subtype;        /**< Temporal subtype */
   int16 flags;          /**< Flags */
   int32 count;          /**< Number of TInstant elements */
   int32 maxcount;       /**< Maximum number of TInstant elements */
   int16 bboxsize;       /**< Size of the bounding box */
   char padding[6];      /**< Not used */
   Span period;          /**< Time span (24 bytes). All bounding boxes start
                              with a period so actually it is also the begining
                              of the bounding box. The extra bytes needed for
                              the bounding box are added upon creation. */
   /* variable-length data follows */
 } TSequence;
 
 #define TSEQUENCE_BBOX_PTR(seq)      ((void *)(&(seq)->period))
 
 /**
  * Structure to represent temporal values of sequence set subtype
  */
 typedef struct
 {
   int32 vl_len_;        /**< Varlena header (do not touch directly!) */
   uint8 temptype;       /**< Temporal type */
   uint8 subtype;        /**< Temporal subtype */
   int16 flags;          /**< Flags */
   int32 count;          /**< Number of TSequence elements */
   int32 totalcount;     /**< Total number of TInstant elements in all
                              composing TSequence elements */
   int32 maxcount;       /**< Maximum number of TSequence elements */
   int16 bboxsize;       /**< Size of the bounding box */
   int16 padding;        /**< Not used */
   Span period;          /**< Time span (24 bytes). All bounding boxes start
                              with a period so actually it is also the begining
                              of the bounding box. The extra bytes needed for
                              the bounding box are added upon creation. */
   /* variable-length data follows */
 } TSequenceSet;
 
 #define TSEQUENCESET_BBOX_PTR(ss)      ((void *)(&(ss)->period))
 
 /**
  * Struct for storing a similarity match
  */
 typedef struct
 {
   int i;
   int j;
 } Match;
 
 /*****************************************************************************/
 
 /**
  * Structure to represent skiplist elements
  */
 
 #define SKIPLIST_MAXLEVEL 32  /**< maximum possible is 47 with current RNG */
 
 typedef struct
 {
   void *value;
   int height;
   int next[SKIPLIST_MAXLEVEL];
 } SkipListElem;
 
 /**
  * Structure to represent skiplists that keep the current state of an aggregation
  */
 typedef struct
 {
   int capacity;
   int next;
   int length;
   int *freed;
   int freecount;
   int freecap;
   int tail;
   void *extra;
   size_t extrasize;
   SkipListElem *elems;
 } SkipList;
 
 /*****************************************************************************/
 
 /**
  * Structure for the in-memory Rtree index
  */
 typedef struct RTree RTree;
 
 /*****************************************************************************
  * Error codes
  *****************************************************************************/
 
 typedef enum
 {
   MEOS_SUCCESS                   = 0,  // Successful operation
 
   MEOS_ERR_INTERNAL_ERROR        = 1,  // Unspecified internal error
   MEOS_ERR_INTERNAL_TYPE_ERROR   = 2,  // Internal type error
   MEOS_ERR_VALUE_OUT_OF_RANGE    = 3,  // Internal out of range error
   MEOS_ERR_DIVISION_BY_ZERO      = 4,  // Internal division by zero error
   MEOS_ERR_MEMORY_ALLOC_ERROR    = 5,  // Internal malloc error
   MEOS_ERR_AGGREGATION_ERROR     = 6,  // Internal aggregation error
   MEOS_ERR_DIRECTORY_ERROR       = 7,  // Internal directory error
   MEOS_ERR_FILE_ERROR            = 8,  // Internal file error
 
   MEOS_ERR_INVALID_ARG           = 10, // Invalid argument
   MEOS_ERR_INVALID_ARG_TYPE      = 11, // Invalid argument type
   MEOS_ERR_INVALID_ARG_VALUE     = 12, // Invalid argument value
   MEOS_ERR_FEATURE_NOT_SUPPORTED = 13, // Feature not currently supported
 
   MEOS_ERR_MFJSON_INPUT          = 20, // MFJSON input error
   MEOS_ERR_MFJSON_OUTPUT         = 21, // MFJSON output error
   MEOS_ERR_TEXT_INPUT            = 22, // Text input error
   MEOS_ERR_TEXT_OUTPUT           = 23, // Text output error
   MEOS_ERR_WKB_INPUT             = 24, // WKB input error
   MEOS_ERR_WKB_OUTPUT            = 25, // WKB output error
   MEOS_ERR_GEOJSON_INPUT         = 26, // GEOJSON input error
   MEOS_ERR_GEOJSON_OUTPUT        = 27, // GEOJSON output error
 
 } errorCode;

 #endif