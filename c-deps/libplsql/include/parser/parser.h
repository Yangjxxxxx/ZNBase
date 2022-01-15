/*-------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

//#include "nodes/parsenodes.h"

typedef struct TypeName
{
    NodeTag     type;
    List       *names;          /* qualified name (list of Value strings) */
    Oid         typeOid;        /* type identified by OID */
    bool        setof;          /* is a set? */
    bool        pct_type;       /* %TYPE specified? */
    List       *typmods;        /* type modifier expression(s) */
    int32       typemod;        /* prespecified type modifier */
    List       *arrayBounds;    /* array bounds */
    int         location;       /* token location, or -1 if unknown */
} TypeName;

typedef enum
{
	BACKSLASH_QUOTE_OFF,
	BACKSLASH_QUOTE_ON,
	BACKSLASH_QUOTE_SAFE_ENCODING
}			BackslashQuoteType;

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern int	backslash_quote;
extern bool escape_string_warning;
extern PGDLLIMPORT bool standard_conforming_strings;


/* Primary entry point for the raw parsing functions */
extern List *raw_parser(const char *str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List *SystemFuncName(char *name);
extern TypeName *SystemTypeName(char *name);

#endif							/* PARSER_H */
