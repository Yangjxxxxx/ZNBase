//
// Created by root on 2020/8/20.
//

#ifndef PLSQL_PLPY_RESULT_H
#define PLSQL_PLPY_RESULT_H

#ifndef PYPGSQL_H
#define PYPGSQL_H
#include "plpgsql.h"
#endif // PYPGSQL_H

#ifndef PYTHON_H
#define PYTHON_H
#include "Python.h"
#endif // PYTHON_H

typedef struct PLyResultObject
{
    PyObject_HEAD
    /* HeapTuple *tuples; */
    PyObject   *nrows;			/* number of rows returned by query */
    PyObject   *rows;			/* data rows, or empty list if no data
								 * returned */
    PyObject   *status;			/* query status, SPI_OK_*, or SPI_ERR_* */
    TupleDesc	tupdesc;
//    PyObject	tupdesc;
} PLyResultObject;

extern PyObject *PLy_result_new(void);

#endif //PLSQL_PLPY_RESULT_H
