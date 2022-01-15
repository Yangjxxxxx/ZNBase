//
// Created by snow on 2020/8/13.
//

#include "pl_python.h"

# define NEXT 6
# define PRIOR 7
# define FIRST 4
# define LAST 5
# define ABSOLUTE 2
# define RELATIVE 3
# define FORWARD 0
# define BACKWARD 1

void fromStrGetHandler(char *handlerStr, Handler_t *handlerPtr) {
    cJSON *handlerRoot = cJSON_Parse(handlerStr);
    cJSON *rcItem = cJSON_GetObjectItem(handlerRoot, "rc");
    cJSON *errItem = cJSON_GetObjectItem(handlerRoot, "ErrMsg");
    cJSON *pointerFDItem = cJSON_GetObjectItem(handlerRoot, "Pointer_func_desc");
    cJSON *pointerItem = cJSON_GetObjectItem(handlerRoot, "pointer");
    cJSON *cursorItem = cJSON_GetObjectItem(handlerRoot, "cursorEngine");
    cJSON *magicItem = cJSON_GetObjectItem(handlerRoot, "magicCode");

    handlerPtr->rc = atoi(cJSON_Print(rcItem));
    handlerPtr->ErrMsg = cJSON_Print(errItem);
    handlerPtr->Pointer_func_desc = atol(cJSON_Print(pointerFDItem));
    handlerPtr->pointer = atol(cJSON_Print(pointerItem));
    handlerPtr->cursorEngine = atol(cJSON_Print(cursorItem));
    handlerPtr->magicCode = atoi(cJSON_Print(magicItem));
}


//函数名：  BEPI_Convert_String_to_plpython_res
//作者：    zhanghao
//日期：    2020-08-20
//功能：    BEPI 接口，用于将语句执行的结果字符串数组返回，并构建相应的plpython结构体
//输入参数：  Handler_t: param 参数
//          valStr : 执行结果对应的字符串数组
//          attrNames : 列名
//          attrsOid : 列类型
//          rows : 结果行数
//          errmsg: 错误信息
//返回值：  PyObject *（PyObject 对象）
PyObject *
BEPI_Convert_String_to_plpython_res(Handler_t handler, char **resStr, char **attrNames, uint attrsOid[], int attrNum,
                                    int rows, int attrSizes[]) {

    PLyResultObject *result;

    result = (PLyResultObject *) PLy_result_new();
    Py_DECREF(result->nrows);
    result->nrows = PyLong_FromUnsignedLongLong(rows);
    Py_DECREF(result->rows);
    result->rows = PyList_New(rows);
    int begin = 0;
    int end = begin + attrNum;
    int i;
    // 用下标拆分字符串数组
    for (i = 0; i < rows; begin += attrNum, i++) {
        PyObject *row = PLyDict_FromTuple(resStr, attrNames, attrsOid, begin, end, attrNum, attrSizes);
        PyList_SetItem(result->rows, i, row);
    }
    Py_DECREF(result->status);
    result->status = PyLong_FromLong(SPI_OK_FETCH);
    //释放过程中使用的内存资源
    for (i = 0; i < attrNum * rows; i++) {
        pfree(resStr[i]);
    }
    for (i = 0; i < attrNum; i++) {
        pfree(attrNames[i]);
    }
    pfree(attrsOid);
    pfree(attrSizes);

    return (PyObject *) result->rows;
}


static PyObject *
Prepare_plan(PyObject *self, PyObject *args) {

    char *handlerStr;
    char *queryString;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_prepare(handler, str) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_prepare() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "ss", &handlerStr, &queryString)) // || !PyArg_ParseTuple(handler_py, "O", &handler))
        return NULL;
    // BEPIPlanIndex planIndex = BEPI_prepare_params(handler, queryString, 0);
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    errorMsg = BepiPreparePlan(*handler, queryString, 0, &planIndex);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return (PyObject *) Py_BuildValue("l", (long) -1);
    }
    pfree(handler->ErrMsg);
    pfree(handler);
    pfree(handlerStr);
    pfree(queryString);
    return (PyObject *) Py_BuildValue("l", (long) planIndex);
}

static PyObject *
ExecutePlan(PyObject *self, PyObject *args) {

    char *handlerStr;
    long planIndex;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "int") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_execute_plan(handler, int) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_execute_plan() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "sl", &handlerStr, &planIndex)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }

    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);
    bool is_query_or_hasReturning = false;
    char *err = BepiExecPlan(*handler, planIndex, &is_query_or_hasReturning);
    if (err != NULL && strlen(err) != 0) {
        PyErr_SetString(PyExc_RuntimeError, err);
        return PyBool_FromLong(0);
    }
    pfree(handler->ErrMsg);
    pfree(handler);
    pfree(handlerStr);
    return PyBool_FromLong(1);
}

static PyObject *
Execute(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *queryString;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_execute(handler, str) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_execute() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "ss", &handlerStr, &queryString)) // || !PyArg_ParseTuple(handler_py, "O", &handler))
        return NULL;
    // BEPIPlanIndex planIndex = BEPI_prepare_params(handler, queryString, 0);
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    errorMsg = BepiPreparePlan(*handler, queryString, 0, &planIndex);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    bool is_query_or_hasReturning = false;
    char *err = BepiExecPlan(*handler, planIndex, &is_query_or_hasReturning);
    if (err != NULL && strlen(err) != 0) {
        PyErr_SetString(PyExc_RuntimeError, err);
        return PyBool_FromLong(0);
    }
    return PyBool_FromLong(1);
}

static PyObject *
ExecutePlanWithReturnRows(PyObject *self, PyObject *args) {
    PyObject *result;
    char *handlerStr;
    long planIndex;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "int") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_execute_plan_with_return(handler, int) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_execute_plan_with_return() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "sl", &handlerStr, &planIndex)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }

    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);
    bool is_query_or_hasReturning = false;
    char *err = BepiExecPlan(*handler, planIndex, &is_query_or_hasReturning);
    if (err != NULL && strlen(err) != 0) {
        PyErr_SetString(PyExc_RuntimeError, err);
        return PyBool_FromLong(0);
    }
    Oid *oids = NULL;
    long rows = 0;
    int colNum = 0;
    char **attrNames;
    int *attrSizes;
    char **retStr = BepiGetReturn(*handler, &oids, &rows, &attrNames, &colNum, &attrSizes);
    result = BEPI_Convert_String_to_plpython_res(*handler, retStr, attrNames, oids, colNum, rows, attrSizes);
    return result;
}

static PyObject *
ExecuteWithReturnRows(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *queryString;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_execute_with_return(handler, str) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_execute_with_return() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "ss", &handlerStr, &queryString)) // || !PyArg_ParseTuple(handler_py, "O", &handler))
        return NULL;
    // BEPIPlanIndex planIndex = BEPI_prepare_params(handler, queryString, 0);
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    errorMsg = BepiPreparePlan(*handler, queryString, 0, &planIndex);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    PyObject *result;
    bool is_query_or_hasReturning = false;
    char *err = BepiExecPlan(*handler, planIndex, &is_query_or_hasReturning);
    if (err != NULL && strlen(err) != 0) {
        PyErr_SetString(PyExc_RuntimeError, err);
        return PyList_New(0);
    }
    Oid *oids = NULL;
    long rows = 0;
    int colNum = 0;
    char **attrNames;
    int *attrSizes;
    char **retStr = BepiGetReturn(*handler, &oids, &rows, &attrNames, &colNum, &attrSizes);
    result = BEPI_Convert_String_to_plpython_res(*handler, retStr, attrNames, oids, colNum, rows, attrSizes);
    return result;
}

static PyObject *
Extest_doppel(char *str) {
    return Py_BuildValue("s", "wo zhen de fu le!\n");
}

static PyObject *
CursorDeclare(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *name;
    char *arglist;
    char *query;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 4) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 2)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 3)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_declare(handler, str, str, str) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_declare() takes 4 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "ssss", &handlerStr, &name, &arglist,
                          &query)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);
    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    int state = BepiCursorDeclare(*handler, name, query, arglist, &errorMsg);
    if (state == 0) {
        if (errorMsg != NULL && strlen(errorMsg) != 0) {
            PyErr_SetString(PyExc_RuntimeError, errorMsg);
            return PyBool_FromLong(0);
        }
    }
    pfree(errorMsg);
    return PyBool_FromLong(1);
}

static PyObject *
CursorOpenWithParamList(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *name;
    char *params;
    bool read_only;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 4) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 2)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 3)->ob_type->tp_name, "bool") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_open_with_paramslist(handler, str, str, bool) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_open_with_paramslist() takes 4 positional arguments but %zd were given",
                argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "sssp", &handlerStr, &name, &params,
                          &read_only)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);
    if (params != NULL) {
    }
    char *query = (char *) palloc(5 + strlen(name) + 1 + strlen(params));
    memset(query, 0, 6 + strlen(name) + strlen(params));
    memcpy(query, "open ", 5);
    strcat(query, name);
    strcat(query, params);

    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    errorMsg = BepiPreparePlan(*handler, query, 0, &planIndex);

    if (strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    bool is_query_or_hasReturning = false;
    errorMsg = BepiExecPlan(*handler, planIndex, &is_query_or_hasReturning);
    // 报错
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    errorMsg = BepiAssignCursorOpen(*handler, name);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    pfree(query);
    pfree(errorMsg);
    return PyBool_FromLong(1);
}

static PyObject *
CursorFind(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *name;
    char *errmg;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_find(handler, str) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_find() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }

    if (!PyArg_ParseTuple(args, "ss", &handlerStr, &name)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    unsigned long cdp = BepiCursorFind(*handler, name, &errmg);
    //CursorDesc cursorDesc = *(CursorDescPtr)cdp;

    if (cdp == 0) {
        return PyBool_FromLong(0);
    } else {
        return PyBool_FromLong(1);
    }

}

static PyObject *
CursorFetch(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *name;
    int direction;
    long count;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 4) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 2)->ob_type->tp_name, "int") == 0) &&
            (strcmp(PyTuple_GetItem(args, 3)->ob_type->tp_name, "int") == 0)) {
            if (!PyArg_ParseTuple(args, "ssil", &handlerStr, &name, &direction,
                                  &count)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
                return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
            }
            if ((direction == NEXT || direction == PRIOR || direction == FIRST || direction == LAST)) {
                char *errorMsg = " Direction NEXT, PRIOR, FIRST, LAST do not need arg count(int) ! ";
                PyErr_SetString(PyExc_TypeError, errorMsg);
                pfree(errorMsg);
                return PyBool_FromLong(0);
            }


        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_fetch(handler, str, int, int) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else if (argsize == 3) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 2)->ob_type->tp_name, "int") == 0)) {
            count = 0;
            if (!PyArg_ParseTuple(args, "ssi", &handlerStr, &name,
                                  &direction)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
                return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
            }
            if ((direction == FORWARD || direction == BACKWARD || direction == ABSOLUTE || direction == RELATIVE)) {
                char *errorMsg = " Direction FORWARD, BACKWARD, ABSOLUTE, RELATIVE need arg count(int) ! ";
                PyErr_SetString(PyExc_TypeError, errorMsg);
                pfree(errorMsg);
                return PyBool_FromLong(0);
            }
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_fetch(handler, str, int) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }

    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_fetch() takes 3 or 4 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);
    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    bool found = BepiCursorFetchPython(*handler, name, direction, count, &errorMsg);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    pfree(errorMsg);
    return PyBool_FromLong(1);
}

static PyObject *
CursorMove(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *name;
    int direction;
    long count;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 4) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 2)->ob_type->tp_name, "int") == 0) &&
            (strcmp(PyTuple_GetItem(args, 3)->ob_type->tp_name, "int") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_move(handler, str, int, int) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_move() takes 4 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "ssil", &handlerStr, &name, &direction,
                          &count)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    char *errorMsg = (char *) palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    bool res = BepiCursorMove(*handler, name, direction, count, &errorMsg);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    pfree(errorMsg);
    if (res) {
        PyBool_FromLong(1);
    } else {
        PyBool_FromLong(0);
    }
}

static PyObject *
CursorGetFetchRow(PyObject *self, PyObject *args) {
    char *handlerStr;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 1) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_get_fetch_row(handler) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_get_fetch_row() takes 1 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "s", &handlerStr)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    PyObject *result;
    Oid *oids = NULL;
    int rows = 0;
    int colNum = 0;
    char **attrNames;
    int *attrSizes;
    char *errorMsg;
    char **retStr = BepiGetCursorFetchRow(*handler, &oids, &rows, &attrNames, &colNum, &attrSizes, &errorMsg);
    if (errorMsg != NULL && strlen(errorMsg) != 0) {
        PyErr_SetString(PyExc_RuntimeError, errorMsg);
        return PyBool_FromLong(0);
    }
    result = BEPI_Convert_String_to_plpython_res(*handler, retStr, attrNames, oids, colNum, rows, attrSizes);
    return result;

}

static PyObject *
CursorClose(PyObject *self, PyObject *args) {
    char *handlerStr;
    char *name;
    char *errmg;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 2) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_cursor_close(handler, str) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_cursor_close() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "ss", &handlerStr, &name)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    BepiCursorClose(*handler, name, &errmg);
    return Py_BuildValue("s", errmg);

}

//static PyObject *
//CastValueToType(PyObject *self, PyObject *args){
//    return NULL;
//}

static PyObject *
GetArgTypeId(PyObject *self, PyObject *args) {
    char *handlerStr;
    int argIndex;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 4) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0) &&
            (strcmp(PyTuple_GetItem(args, 1)->ob_type->tp_name, "int") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_getargtypeid(handler, int) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_getargtypeid() takes 2 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "si", &handlerStr, &argIndex)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    unsigned int id;
    id = BepiGetArgTypeID(*handler, argIndex);
    return Py_BuildValue("I", id);


}

static PyObject *
GetArgCount(PyObject *self, PyObject *args) {
    char *handlerStr;
    Py_ssize_t argsize = PyTuple_Size(args);
    if (argsize == 1) {
        if ((strcmp(PyTuple_GetItem(args, 0)->ob_type->tp_name, "str") == 0)) {
        } else {
            char *errorMsg = " Wrong arg types of BEPI_getargcount(handler) ";
            PyErr_SetString(PyExc_TypeError, errorMsg);
            pfree(errorMsg);
            return PyBool_FromLong(0);
        }
    } else {
        char *errorMsg = (char *) palloc(ERRSIZE);
        memset(errorMsg, 0, ERRSIZE);
        sprintf(errorMsg, " BEPI_getargcount() takes 1 positional arguments but %zd were given", argsize);
        PyErr_SetString(PyExc_TypeError, errorMsg);
        pfree(errorMsg);
        return PyBool_FromLong(0);
    }
    if (!PyArg_ParseTuple(args, "s", &handlerStr)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
    }
    Handler_t *handler = palloc(sizeof(Handler_t));
    handler->ErrMsg = palloc(sizeof(200));
    fromStrGetHandler(handlerStr, handler);

    int count;
    count = BepiGetArgCount(*handler);
    return Py_BuildValue("i", count);
}

//static PyObject *
//ResultCodeToString(PyObject *self, PyObject *args){
//    int code;
//    if (!PyArg_ParseTuple(args, "i", &code)) { // || !PyArg_ParseTuple(handler_py, "O", &handler)
//        return (PyObject *) Py_BuildValue("s", "error happen when PyArg_ParseTuple");
//    }
//    static char buf[64];
//
//    switch (code)
//    {
//        case SPI_ERROR_CONNECT:
//            return "SPI_ERROR_CONNECT";
//        case SPI_ERROR_COPY:
//            return "SPI_ERROR_COPY";
//        case SPI_ERROR_OPUNKNOWN:
//            return "SPI_ERROR_OPUNKNOWN";
//        case SPI_ERROR_UNCONNECTED:
//            return "SPI_ERROR_UNCONNECTED";
//        case SPI_ERROR_ARGUMENT:
//            return "SPI_ERROR_ARGUMENT";
//        case SPI_ERROR_PARAM:
//            return "SPI_ERROR_PARAM";
//        case SPI_ERROR_TRANSACTION:
//            return "SPI_ERROR_TRANSACTION";
//        case SPI_ERROR_NOATTRIBUTE:
//            return "SPI_ERROR_NOATTRIBUTE";
//        case SPI_ERROR_NOOUTFUNC:
//            return "SPI_ERROR_NOOUTFUNC";
//        case SPI_ERROR_TYPUNKNOWN:
//            return "SPI_ERROR_TYPUNKNOWN";
//        case SPI_ERROR_REL_DUPLICATE:
//            return "SPI_ERROR_REL_DUPLICATE";
//        case SPI_ERROR_REL_NOT_FOUND:
//            return "SPI_ERROR_REL_NOT_FOUND";
//        case SPI_OK_CONNECT:
//            return "SPI_OK_CONNECT";
//        case SPI_OK_FINISH:
//            return "SPI_OK_FINISH";
//        case SPI_OK_FETCH:
//            return "SPI_OK_FETCH";
//        case SPI_OK_UTILITY:
//            return "SPI_OK_UTILITY";
//        case SPI_OK_SELECT:
//            return "SPI_OK_SELECT";
//        case SPI_OK_SELINTO:
//            return "SPI_OK_SELINTO";
//        case SPI_OK_INSERT:
//            return "SPI_OK_INSERT";
//        case SPI_OK_DELETE:
//            return "SPI_OK_DELETE";
//        case SPI_OK_UPDATE:
//            return "SPI_OK_UPDATE";
//        case SPI_OK_CURSOR:
//            return "SPI_OK_CURSOR";
//        case SPI_OK_INSERT_RETURNING:
//            return "SPI_OK_INSERT_RETURNING";
//        case SPI_OK_DELETE_RETURNING:
//            return "SPI_OK_DELETE_RETURNING";
//        case SPI_OK_UPDATE_RETURNING:
//            return "SPI_OK_UPDATE_RETURNING";
//        case SPI_OK_REWRITTEN:
//            return "SPI_OK_REWRITTEN";
//        case SPI_OK_REL_REGISTER:
//            return "SPI_OK_REL_REGISTER";
//        case SPI_OK_REL_UNREGISTER:
//            return "SPI_OK_REL_UNREGISTER";
//    }
//    /* Unrecognized code ... return something useful ... */
//    sprintf(buf, "Unrecognized SPI code %d", code);
//    return Py_BuildValue("s",buf);
//}



static PyMethodDef
        PLy_methods[] =
        {
                //{"doppel",                          Extest_doppel,             METH_VARARGS},
                {"BEPI_prepare",                    Prepare_plan,              METH_VARARGS},
                {"BEPI_execute",                    Execute,                   METH_VARARGS},
                {"BEPI_execute_plan",               ExecutePlan,               METH_VARARGS},
                {"BEPI_execute_with_return",        ExecuteWithReturnRows,     METH_VARARGS},
                {"BEPI_execute_plan_with_return",   ExecutePlanWithReturnRows, METH_VARARGS},
                {"BEPI_cursor_declare",             CursorDeclare,             METH_VARARGS},
                {"BEPI_cursor_open_with_paramlist", CursorOpenWithParamList,   METH_VARARGS},
                {"BEPI_cursor_find",                CursorFind,                METH_VARARGS},
                {"BEPI_cursor_fetch",               CursorFetch,               METH_VARARGS},
                {"BEPI_cursor_move",                CursorMove,                METH_VARARGS},
                {"BEPI_cursor_get_fetch_row",       CursorGetFetchRow,         METH_VARARGS},
                {"BEPI_cursor_close",               CursorClose,               METH_VARARGS},
                {"BEPI_getargtypeid",               GetArgTypeId,              METH_VARARGS},
                {"BEPI_getargcount",                GetArgCount,               METH_VARARGS},
//                {"BEPI_scroll_cursor_fetch",        ScrollCursorFetch,         METH_VARARGS},
//                {"BEPI_scroll_cursor_move",         ScrollCursorMove,          METH_VARARGS},
                {NULL, NULL},
        };

static struct PyModuleDef Python_bepi = {
        PyModuleDef_HEAD_INIT,
        "Pythonbepi",            /* name of module */
        NULL,                    /* module documentation, may be NULL */
        -1,                      /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
        PLy_methods              /* A pointer to a table of module-level functions, described by PyMethodDef values. Can be NULL if no functions are present. */
};

PyObject *Pythonbepi(void) {
    PyObject *module;
    module = PyModule_Create(&Python_bepi);
    if (module == NULL)
        return NULL;
    PyModule_AddIntConstant(module, "NEXT", 6);
    PyModule_AddIntConstant(module, "PRIOR", 7);
    PyModule_AddIntConstant(module, "FIRST", 4);
    PyModule_AddIntConstant(module, "LAST", 5);
    PyModule_AddIntConstant(module, "ABSOLUTE", 2);
    PyModule_AddIntConstant(module, "RELATIVE", 3);
    PyModule_AddIntConstant(module, "FORWARD", 0);
    PyModule_AddIntConstant(module, "BACKWARD", 1);
    return module;
}


void PyInit_Pythonbepi() {
    Pythonbepi();
//    Py_InitModule("Pythonbepi", PLy_methods);
//     // #if PY_MAJOR_VERSION >= 3
//     static PyModuleDef PLy_module = {
//         PyModuleDef_HEAD_INIT,
//         .m_name = "Pythonbepi",
//         .m_size = -1,
//         .m_methods = PLy_methods,
//     };
//
////     PyObject   *m;
//     PyModule_Create(&PLy_module);


    // if (m == NULL)
    //     return NULL;
    //
    //
    // return m;
}
