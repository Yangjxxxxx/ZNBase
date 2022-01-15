//
// Created by root on 2020/8/20.
//

#ifndef PLSQL_PLPY_TYPE_H
#define PLSQL_PLPY_TYPE_H

#ifndef PYPGSQL_H
#define PYPGSQL_H
#include "plpgsql.h"
#endif // PYPGSQL_H

#ifndef PYTHON_H
#define PYTHON_H
#endif
// PYTHON_H

#include "Python.h"




typedef struct PLyDatumToOb PLyDatumToOb;	/* forward reference */

typedef PyObject *(*PLyDatumToObFunc) (PLyDatumToOb *arg, Datum val);

typedef struct PLyScalarToOb
{
    FmgrInfo	typfunc;		/* lookup info for type's output function */
} PLyScalarToOb;

typedef struct PLyArrayToOb
{
    PLyDatumToOb *elm;			/* conversion info for array's element type */
} PLyArrayToOb;

typedef struct PLyTupleToOb
{
    /* If we're dealing with a RECORD type, actual descriptor is here: */
    TupleDesc	recdesc;
    /* If we're dealing with a named composite type, these fields are set: */
    // TypeCacheEntry *typentry;	/* typcache entry for type */
    uint64		tupdescid;		/* last tupdesc identifier seen in typcache */
    /* These fields are NULL/0 if not yet set: */
    PLyDatumToOb *atts;			/* array of per-column conversion info */
    int			natts;			/* length of array */
} PLyTupleToOb;

typedef struct PLyTransformToOb
{
    FmgrInfo	typtransform;	/* lookup info for from-SQL transform func */
} PLyTransformToOb;

struct PLyDatumToOb
{
    PLyDatumToObFunc func;		/* conversion control function */
    uint 			typoid;			/* OID of the source type */
    int32		typmod;			/* typmod of the source type */
    bool		typbyval;		/* its physical representation details */
    int16		typlen;
    char		typalign;
    MemoryContext mcxt;			/* context this info is stored in */
    union						/* conversion-type-specific data */
    {
        PLyScalarToOb scalar;
        PLyArrayToOb array;
        PLyTupleToOb tuple;
        PLyTransformToOb transform;
    }			u;
};

/* conversion from Datums to Python objects */
PyObject *PLyBool_FromBool(PLyDatumToOb *arg, Datum d);
PyObject *PLyFloat_FromFloat4(PLyDatumToOb *arg, Datum d);
PyObject *PLyFloat_FromFloat8(PLyDatumToOb *arg, Datum d);
PyObject *PLyDecimal_FromNumeric(PLyDatumToOb *arg, Datum d);
PyObject *PLyInt_FromInt16(PLyDatumToOb *arg, Datum d);
PyObject *PLyInt_FromInt32(PLyDatumToOb *arg, Datum d);
PyObject *PLyLong_FromInt64(PLyDatumToOb *arg, Datum d);
PyObject *PLyLong_FromOid(PLyDatumToOb *arg, Datum d);
// static PyObject *PLyBytes_FromBytea(PLyDatumToOb *arg, Datum d);
// static PyObject *PLyString_FromScalar(PLyDatumToOb *arg, Datum d);
// static PyObject *PLyObject_FromTransform(PLyDatumToOb *arg, Datum d);
// static PyObject *PLyList_FromArray(PLyDatumToOb *arg, Datum d);
// static PyObject *PLyList_FromArray_recurse(PLyDatumToOb *elm, int *dims, int ndim, int dim,
//                                            char **dataptr_p, bits8 **bitmap_p, int *bitmask_p);
// static PyObject *PLyDict_FromComposite(PLyDatumToOb *arg, Datum d);
// static PyObject *PLyDict_FromTuple(PLyDatumToOb *arg, HeapTuple tuple, TupleDesc desc, bool include_generated);
PyObject *PLyDict_FromTuple(char **resStr, char **attrNames, uint attrOids[], int begin, int end, int attrNum, int attrSizes[]);

#endif //PLSQL_PLPY_TYPE_H
