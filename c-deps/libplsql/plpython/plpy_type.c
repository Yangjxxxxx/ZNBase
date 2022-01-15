//
// Created by root on 2020/8/20.
//

#include "pl_python.h"


/*
 * Special-purpose input converters.
 */

PyObject *
PLyBool_FromBool(PLyDatumToOb *arg, Datum d) {
    if (DatumGetBool(d))
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

PyObject *
PLyFloat_FromFloat4(PLyDatumToOb *arg, Datum d) {
    return PyFloat_FromDouble(DatumGetFloat4(d));
}

PyObject *
PLyFloat_FromFloat8(PLyDatumToOb *arg, Datum d) {
    return PyFloat_FromDouble(DatumGetFloat8(d));
}

PyObject *
PLyDecimal_FromNumeric(PLyDatumToOb *arg, Datum d) {
    static PyObject *decimal_constructor;
    char *str;
    PyObject *pyvalue;

    /* Try to import cdecimal.  If it doesn't exist, fall back to decimal. */
    if (!decimal_constructor) {
        PyObject *decimal_module;

        decimal_module = PyImport_ImportModule("cdecimal");
        if (!decimal_module) {
            PyErr_Clear();
            decimal_module = PyImport_ImportModule("decimal");
        }
        // if (!decimal_module)
        // ereport(ERROR, (errmsg("could not import a module for Decimal constructor")));
        // PLy_elog(ERROR, "could not import a module for Decimal constructor");

        decimal_constructor = PyObject_GetAttrString(decimal_module, "Decimal");
        // if (!decimal_constructor)
        // ereport(ERROR, (errmsg("no Decimal attribute in module")));
        // PLy_elog(ERROR, "no Decimal attribute in module");
    }

    // str = DatumGetCString(DirectFunctionCall1(numeric_out, d));
    str = "123566789";
    pyvalue = PyObject_CallFunction(decimal_constructor, "s", str);
    if (!pyvalue)
        // ereport(ERROR, (errmsg("conversion from numeric to Decimal failed")));
        // PLy_elog(ERROR, "conversion from numeric to Decimal failed");

        return pyvalue;
}

PyObject *
PLyInt_FromInt16(PLyDatumToOb *arg, Datum d) {
    return PyLong_FromLong(DatumGetInt16(d));
}

PyObject *
PLyInt_FromInt32(PLyDatumToOb *arg, Datum d) {
    return PyLong_FromLong(DatumGetInt32(d));
}

PyObject *
PLyLong_FromInt64(PLyDatumToOb *arg, Datum d) {
    return PyLong_FromLongLong(DatumGetInt64(d));
}

PyObject *
PLyLong_FromOid(PLyDatumToOb *arg, Datum d) {
    return PyLong_FromUnsignedLong(DatumGetObjectId(d));
}

// comment it now, uncomment in future
//
// static PyObject *
// PLyBytes_FromBytea(PLyDatumToOb *arg, Datum d)
// {
//     text	   *txt = DatumGetByteaPP(d);
//     char	   *str = VARDATA_ANY(txt);
//     size_t		size = VARSIZE_ANY_EXHDR(txt);
//
//     return PyBytes_FromStringAndSize(str, size);
// }



/*
 * Generic input conversion using a SQL type's output function.
 */
// comment it now, uncomment in future
//
// static PyObject *
// PLyString_FromScalar(PLyDatumToOb *arg, Datum d)
// {
//     char	   *x = OutputFunctionCall(&arg->u.scalar.typfunc, d);
//     PyObject   *r = PyString_FromString(x);
//
//     pfree(x);
//     return r;
// }

/*
 * Convert using a from-SQL transform function.
 */
// static PyObject *
// PLyObject_FromTransform(PLyDatumToOb *arg, Datum d)
// {
//     Datum		t;
//
//     t = FunctionCall1(&arg->u.transform.typtransform, d);
//     return (PyObject *) DatumGetPointer(t);
// }

/*
 * Convert a SQL array to a Python list.
 */
// static PyObject *
// PLyList_FromArray(PLyDatumToOb *arg, Datum d)
// {
//     ArrayType  *array = DatumGetArrayTypeP(d);
//     PLyDatumToOb *elm = arg->u.array.elm;
//     int			ndim;
//     int		   *dims;
//     char	   *dataptr;
//     bits8	   *bitmap;
//     int			bitmask;
//
//     if (ARR_NDIM(array) == 0)
//         return PyList_New(0);
//
//     /* Array dimensions and left bounds */
//     ndim = ARR_NDIM(array);
//     dims = ARR_DIMS(array);
//     Assert(ndim < MAXDIM);
//
//     /*
//      * We iterate the SQL array in the physical order it's stored in the
//      * datum. For example, for a 3-dimensional array the order of iteration
//      * would be the following: [0,0,0] elements through [0,0,k], then [0,1,0]
//      * through [0,1,k] till [0,m,k], then [1,0,0] through [1,0,k] till
//      * [1,m,k], and so on.
//      *
//      * In Python, there are no multi-dimensional lists as such, but they are
//      * represented as a list of lists. So a 3-d array of [n,m,k] elements is a
//      * list of n m-element arrays, each element of which is k-element array.
//      * PLyList_FromArray_recurse() builds the Python list for a single
//      * dimension, and recurses for the next inner dimension.
//      */
//     dataptr = ARR_DATA_PTR(array);
//     bitmap = ARR_NULLBITMAP(array);
//     bitmask = 1;
//
//     return PLyList_FromArray_recurse(elm, dims, ndim, 0,
//                                      &dataptr, &bitmap, &bitmask);
// }

// static PyObject *
// PLyList_FromArray_recurse(PLyDatumToOb *elm, int *dims, int ndim, int dim,
//                           char **dataptr_p, bits8 **bitmap_p, int *bitmask_p)
// {
//     int			i;
//     PyObject   *list;
//
//     list = PyList_New(dims[dim]);
//     if (!list)
//         return NULL;
//
//     if (dim < ndim - 1)
//     {
//         /* Outer dimension. Recurse for each inner slice. */
//         for (i = 0; i < dims[dim]; i++)
//         {
//             PyObject   *sublist;
//
//             sublist = PLyList_FromArray_recurse(elm, dims, ndim, dim + 1,
//                                                 dataptr_p, bitmap_p, bitmask_p);
//             PyList_SET_ITEM(list, i, sublist);
//         }
//     }
//     else
//     {
//         /*
//          * Innermost dimension. Fill the list with the values from the array
//          * for this slice.
//          */
//         char	   *dataptr = *dataptr_p;
//         bits8	   *bitmap = *bitmap_p;
//         int			bitmask = *bitmask_p;
//
//         for (i = 0; i < dims[dim]; i++)
//         {
//             /* checking for NULL */
//             if (bitmap && (*bitmap & bitmask) == 0)
//             {
//                 Py_INCREF(Py_None);
//                 PyList_SET_ITEM(list, i, Py_None);
//             }
//             else
//             {
//                 Datum		itemvalue;
//
//                 itemvalue = fetch_att(dataptr, elm->typbyval, elm->typlen);
//                 PyList_SET_ITEM(list, i, elm->func(elm, itemvalue));
//                 dataptr = att_addlength_pointer(dataptr, elm->typlen, dataptr);
//                 dataptr = (char *) att_align_nominal(dataptr, elm->typalign);
//             }
//
//             /* advance bitmap pointer if any */
//             if (bitmap)
//             {
//                 bitmask <<= 1;
//                 if (bitmask == 0x100 /* (1<<8) */ )
//                 {
//                     bitmap++;
//                     bitmask = 1;
//                 }
//             }
//         }
//
//         *dataptr_p = dataptr;
//         *bitmap_p = bitmap;
//         *bitmask_p = bitmask;
//     }
//
//     return list;
// }

/*
 * Convert a composite SQL value to a Python dict.
 */
// static PyObject *
// PLyDict_FromComposite(PLyDatumToOb *arg, Datum d)
// {
//     PyObject   *dict;
//     HeapTupleHeader td;
//     Oid			tupType;
//     int32		tupTypmod;
//     TupleDesc	tupdesc;
//     HeapTupleData tmptup;
//
//     td = DatumGetHeapTupleHeader(d);
//     /* Extract rowtype info and find a tupdesc */
//     tupType = HeapTupleHeaderGetTypeId(td);
//     tupTypmod = HeapTupleHeaderGetTypMod(td);
//     tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
//
//     /* Set up I/O funcs if not done yet */
//     PLy_input_setup_tuple(arg, tupdesc,
//                           PLy_current_execution_context()->curr_proc);
//
//     /* Build a temporary HeapTuple control structure */
//     tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
//     tmptup.t_data = td;
//
//     dict = PLyDict_FromTuple(arg, &tmptup, tupdesc, true);
//
//     ReleaseTupleDesc(tupdesc);
//
//     return dict;
// }

/*
 * Transform a tuple into a Python dict object.
 */
// static PyObject *
// PLyDict_FromTuple(PLyDatumToOb *arg, HeapTuple tuple, TupleDesc desc, bool include_generated)
// {
//     PyObject   *volatile dict;
//
//     /* Simple sanity check that desc matches */
//     Assert(desc->natts == arg->u.tuple.natts);
//
//     dict = PyDict_New();
//     if (dict == NULL)
//         return NULL;
//
//     PG_TRY();
//             {
//                 int			i;
//
//                 for (i = 0; i < arg->u.tuple.natts; i++)
//                 {
//                     PLyDatumToOb *att = &arg->u.tuple.atts[i];
//                     Form_pg_attribute attr = TupleDescAttr(desc, i);
//                     char	   *key;
//                     Datum		vattr;
//                     bool		is_null;
//                     PyObject   *value;
//
//                     if (attr->attisdropped)
//                         continue;
//
//                     if (attr->attgenerated)
//                     {
//                         /* don't include unless requested */
//                         if (!include_generated)
//                             continue;
//                     }
//
//                     key = NameStr(attr->attname);
//                     vattr = heap_getattr(tuple, (i + 1), desc, &is_null);
//
//                     if (is_null)
//                         PyDict_SetItemString(dict, key, Py_None);
//                     else
//                     {
//                         value = att->func(att, vattr);
//                         PyDict_SetItemString(dict, key, value);
//                         Py_DECREF(value);
//                     }
//                 }
//             }
//         PG_CATCH();
//             {
//                 Py_DECREF(dict);
//                 PG_RE_THROW();
//             }
//     PG_END_TRY();
//
//     return dict;
// }


/*
 * Transform a tuple into a Python dict object.
 */
PyObject *
PLyDict_FromTuple(char **resStr, char **attrNames, uint attrOids[], int begin, int end, int attrNum, int attrSizes[]) {
    PyObject *volatile dict;

    /* Simple sanity check that desc matches */
    // Assert(desc->natts == arg->u.tuple.natts);

    dict = PyDict_New();
    if (dict == NULL)
        return NULL;

    // PG_TRY();
    {
        int i;
        for (i = 0; i < attrNum; i++) {
            // PLyDatumToOb *att = &arg->u.tuple.atts[i];
            // Form_pg_attribute attr = TupleDescAttr(desc, i);
            char *key;
            // Datum		vattr;
            bool is_null = false;
            PyObject *value;
            Datum tempDatum;
            //
            // if (attr->attisdropped)
            //     continue;
            //
            // if (attr->attgenerated)
            // {
            //     /* don't include unless requested */
            //     if (!include_generated)
            //         continue;
            // }

            // key = NameStr(attr->attname);
            key = attrNames[i];
            char *valStr = resStr[begin + i];
            int length = attrSizes[begin + i];
            switch (attrOids[begin + i]) {
                // case INT2OID: {
                //     int16 shori = (int16)(atoi(resStr));
                //     value = ;
                //     break;
                // }
                // case INT4OID:
                case INT8OID: {
                    int intVal = atoi(valStr);
                    // tempDatum = Int8GetDatum(intVal));
                    // value = PLyInt_FromInt32(tempDatum);
                    value = PyLong_FromLong(intVal);
                    break;
                }
                case FLOAT8OID:
                case NUMERICOID: {
                    double doubleNum = atof(valStr);
                    // tempDatum = Float8GetDatum(doubleNum);
                    // value = PLyFloat_FromFloat8(tempDatum);
                    value = PyFloat_FromDouble(doubleNum);
                    break;
                }
                case TEXTOID: {
                    // tempDatum = CStringGetDatum(valStr);
                    value = PyUnicode_FromString(valStr);
                    break;
                }
                case BYTEAOID: {
                    value = PyBytes_FromStringAndSize(valStr, length);
                    break;
                }
                case BOOLOID: {
                    char *f = "false";
                    int r = strcmp(f, valStr);
                    value = PyBool_FromLong(r);
                    break;
                }
                case DATEOID:
                case TIMEOID:
                case TIMESTAMPOID:
                case TIMESTAMPTZOID: {
                    value = PyUnicode_FromString(valStr);
                    break;
                }

                case UNKNOWNOID: {
                    value = Py_None;
                    break;
                }

                default: {
                    value = PyUnicode_FromString("");
                    // ereport(ERROR, (errmsg("not implement yet!\n")));
                    break;
                }

            }
            // vattr = heap_getattr(tuple, (i + 1), desc, &is_null);

            if (is_null)
                PyDict_SetItemString(dict, key, Py_None);
            else {
                // value = att->func(att, vattr);
                PyDict_SetItemString(dict, key, value);
                Py_DECREF(value);
            }
        }
    }
    //     PG_CATCH();
    //         {
    //             Py_DECREF(dict);
    //             PG_RE_THROW();
    //         }
    // PG_END_TRY();

    return dict;
}