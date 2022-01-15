/*-------------------------------------------------------------------------
 *
 * pl_handler.c		- Handler for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "plpgsql.h"

#ifndef __APPLE__
#include <Python.h>
#endif

#include <string.h>
#include "pthread.h"
#include <dlfcn.h>
#include "../include/utils/numeric.h"


/* Custom GUC variable */
// structure for next variable
struct config_enum_entry {
    const char *name;
    int val;
    bool hidden;
};

int plpgsql_variable_conflict = PLPGSQL_RESOLVE_ERROR;

bool plpgsql_print_strict_params = false;

bool plpgsql_check_asserts = true;

char *plpgsql_extra_warnings_string = NULL;
char *plpgsql_extra_errors_string = NULL;
int plpgsql_extra_warnings;
int plpgsql_extra_errors;
/* Hook for plugins */
PLpgSQL_plugin **plpgsql_plugin_ptr = NULL;

// a global variable for plclang to close unclosed function when error happen in executing
void *funcHandle = NULL;

#ifndef __APPLE__
static bool pyIsOpened = false;

/* Load a symbol from a module */
PyObject *import_name(const char *modname, const char *symbol) {
    PyObject *u_name, *module;
    u_name = Py_BuildValue("s", modname);
    // u_name = PyUnicode_FromString(modname);
    module = PyImport_Import(u_name);
    // module = PyImport_ImportModule(modname);
    if (!module) {
        PyErr_Print();
    }
    Py_DECREF(u_name);
    return PyObject_GetAttrString(module, symbol);
}
#endif


/*
 * _PG_init()			- library load-time initialization
 *
 * DO NOT make this static nor change its name!
 */
void _PG_init(void) {
    /* Be sure we do initialization only once (should be redundant now) */
    // pthread_mutex_init(&mutex, NULL);
    static bool inited = false;
    if (inited)
        return;
    MemoryContextInit();
    /* Set up a rendezvous point with optional instrumentation plugin */
    plpgsql_plugin_ptr = NULL;
    inited = true;
}

//int PyGILState_Check2(void) {
//    PyThreadState *tstate = _PyThreadState_Current;
//    return tstate && (tstate == PyGILState_GetThisThreadState());
//}

/* ----------
 * plpgsql_call_handler
 *
 * The PostgreSQL function manager and trigger manager
 * call this function for execution of PL/pgSQL procedures.
 * ----------
 */

typedef struct paras {
    FunctionCallInfo fcinfo;
    Handler_t handler;
    PLpgSQL_function *func;
    bool *nonatomic;
    char *errorMsg;
} paras;

int index1234 = 0;

#ifndef __APPLE__
char *get_python_traceback(bool *errOccured) {
    PyObject *pErr = PyErr_Occurred();
    *errOccured = false;
    if (pErr != NULL) {
        *errOccured = true;
        PyObject *ptype, *pvalue, *ptraceback;
        PyObject *pystr, *tmodule_name, *pyth_module, *pyth_func;
        char *str;

        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
        pystr = PyObject_Str(pvalue);
        PyArg_Parse(pystr, "s", &str);

        tmodule_name = PyUnicode_FromString("traceback");
        pyth_module = PyImport_Import(tmodule_name);
        Py_DECREF(tmodule_name);

        if (pyth_module == NULL) {
            return "Cannot get traceback messege ";
        }
        pyth_func = PyObject_GetAttrString(pyth_module, "format_exception");
        if (pyth_func && PyCallable_Check(pyth_func)) {
            PyObject *pyth_val;
            PyObject *pargs = PyTuple_New(3);
            PyTuple_SetItem(pargs, 0, ptype);
            if (pvalue) {
                PyTuple_SetItem(pargs, 1, pvalue);
            } else {
                PyTuple_SetItem(pargs, 1, Py_None);
            }
            if (ptraceback) {
                PyTuple_SetItem(pargs, 2, ptraceback);
            } else {
                PyTuple_SetItem(pargs, 2, Py_None);
            }
            pyth_val = PyObject_CallObject(pyth_func, pargs);
            //pyth_val = PyObject_CallFunctionObjArgs(pyth_func, ptype, pvalue, ptraceback, NULL);
            PyObject *py_val = PyUnicode_Join(PyUnicode_FromString(""), pyth_val);
            pystr = PyObject_Str(py_val);
            PyArg_Parse(pystr, "s", &str);
            Py_XDECREF(pyth_val);
            Py_XDECREF(py_val);
            Py_XDECREF(pargs);
            PyErr_Clear();
            return str;
        }
        Py_XDECREF(ptype);
        Py_XDECREF(pvalue);
        Py_XDECREF(ptraceback);
        Py_XDECREF(pystr);
    }

    return "";
}

static
void *plpgsql_python_script(void *a) {

    PyGILState_STATE gstate = PyGILState_Ensure();

    paras *pythonParas = (paras *) a;
    FunctionCallInfo fcinfo = pythonParas->fcinfo;
    Handler_t handler = pythonParas->handler;
    struct PLpgSQL_function *func = pythonParas->func;
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.append('./')");
    FormData_pg_proc meta = SearchFunctionMetaCache(handler, PROC_CATCH_ID);
    char *file_path = meta.file_path;
    char *module_name = meta.file_name;
    char *sentence = palloc(20 * sizeof(char) + strlen(file_path));
    sprintf(sentence, "sys.path.append('%s')", file_path);
    PyRun_SimpleString(sentence);
    PyObject *pName = NULL;
    PyObject *pModule = NULL;
    PyObject *pDict = NULL;
    PyObject *pFunc = NULL;
    PyObject *pArgs = NULL;
    PyObject *pRes = NULL;
    PyObject *volatile plrv_so = NULL;
    PyObject *pModuleDict = NULL;

    pName = PyUnicode_FromString(module_name);
    pModuleDict = PyImport_GetModuleDict();
    int keyindict = PyDict_Contains(pModuleDict, pName);
    if (keyindict) {
        pModule = PyImport_GetModule(pName);
        pModule = PyImport_ReloadModule(pModule);
    } else {
        pModule = PyImport_Import(pName);
    }
    Py_DECREF(pName);
    if (!pModule) {
        PyErr_Print();
        char *str = (char *) palloc(128);
        if (keyindict) {
            sprintf(str, "Reload Module %s failed!\n", module_name);
        } else {
            sprintf(str, "Load Module %s failed!\n", module_name);
        }
        pythonParas->errorMsg = str;
        PyGILState_Release(gstate);
        pthread_exit(NULL);
        return NULL;
    }
    pDict = PyModule_GetDict(pModule);
    if (!pDict) {
        char *str = "Can't find dict in python_func!\n";
        pythonParas->errorMsg = str;
        PyGILState_Release(gstate);
        pthread_exit(NULL);
        return NULL;
    }
    char *funcName = meta.proname.data;
    pFunc = PyDict_GetItemString(pDict, funcName);

    if (!pFunc || !PyCallable_Check(pFunc)) {
        char *str = (char *) palloc(128);
        sprintf(str, "Can't find function %s!\n", funcName);
        pythonParas->errorMsg = str;
        PyGILState_Release(gstate);
        pthread_exit(NULL);
        return NULL;
    }
    if (!PyCallable_Check(pFunc)) {
        printf("Can't find dict in python_func!\n");
        char *str = "expected a callable function";
        pythonParas->errorMsg = str;
        PyGILState_Release(gstate);
        pthread_exit(NULL);
        return NULL;
    }

    // 构造handler参数
    cJSON *handlerJSON = cJSON_CreateObject();
    cJSON_AddItemToObject(handlerJSON, "rc", cJSON_CreateNumber(handler.rc));
    cJSON_AddItemToObject(handlerJSON, "ErrMsg", cJSON_CreateString(handler.ErrMsg));
    cJSON_AddItemToObject(handlerJSON, "Pointer_func_desc", cJSON_CreateNumber(handler.Pointer_func_desc));
    cJSON_AddItemToObject(handlerJSON, "pointer", cJSON_CreateNumber(handler.pointer));
    cJSON_AddItemToObject(handlerJSON, "cursorEngine", cJSON_CreateNumber(handler.cursorEngine));
    cJSON_AddItemToObject(handlerJSON, "magicCode", cJSON_CreateNumber(handler.magicCode));
    char *handlerStr = cJSON_Print(handlerJSON);

    pArgs = PyTuple_New(func->fn_nargs + 1);
    for (int i = 0; i < func->fn_nargs + 1; i++) {
        if (i == 0) {
            PyTuple_SetItem(pArgs, i, Py_BuildValue("s", handlerStr));
        } else {
            bool is_null;
            is_null = false;

            switch (meta.argtypes[i - 1]) {
                case INT8OID:
                case INT4OID:
                case INT2OID: {
                    char *para = GetNthParam(handler, i - 1, &is_null);
                    if (!is_null) {
                        PyTuple_SetItem(pArgs, i, Py_BuildValue("L", atoll(para)));
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_None);
                    }
                    break;
                }
                case NUMERICOID: {
                    PyObject *pDecimalName = PyUnicode_FromString("decimal");
                    PyObject *pDecimalModule = PyImport_GetModule(pDecimalName);
                    if (pDecimalModule == NULL) {
                        pythonParas->errorMsg = "Package decimal has not been imported !";
                        PyGILState_Release(gstate);
                        pthread_exit(NULL);
                        return NULL;
                    }
                    char *para = GetNthParam(handler, i - 1, &is_null);
                    if (!is_null) {
                        PyObject *pDecimalValue = PyObject_CallMethod(pDecimalModule, "Decimal", "s", para);
                        PyTuple_SetItem(pArgs, i, pDecimalValue);
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_None);
                    }
                    break;
                }

                case FLOAT4OID:
                case FLOAT8OID: {

                    char *para = GetNthParam(handler, i - 1, &is_null);
                    if (!is_null) {
                        PyTuple_SetItem(pArgs, i, Py_BuildValue("f", atof(para)));
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_None);
                    }
                    break;
                }
                case TEXTOID:
                case VARCHAROID: {
                    char *para = GetNthParam(handler, i - 1, &is_null);
                    if (!is_null) {
                        PyTuple_SetItem(pArgs, i, Py_BuildValue("s", (char *) para));
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_None);
                    }
                    break;
                }
                case INT8ARRAYOID:
                case INT4ARRAYOID:
                case INT2ARRAYOID: {
                    int nlength = GetNthParamListLength(handler, i - 1);
                    if (nlength == -1) {
                        PyTuple_SetItem(pArgs, i, Py_None);
                        continue;
                    }
                    PyObject *pList = PyList_New(nlength);
                    for (int j = 0; j < nlength; j++) {
                        char *temp = GetNthParamListI(handler, i - 1, j, &is_null);
                        if (!is_null) {
                            PyList_SetItem(pList, j, Py_BuildValue("L", atoll(temp)));
                        } else {
                            PyList_SetItem(pList, j, Py_None);
                        }
                    }
                    PyTuple_SetItem(pArgs, i, pList);
                    break;
                }
                case FLOAT4ARRAYOID:
                case FLOAT8ARRAYOID: {
                    int nlength = GetNthParamListLength(handler, i - 1);
                    if (nlength == -1) {
                        PyTuple_SetItem(pArgs, i, Py_None);
                        continue;
                    }
                    PyObject *pList = PyList_New(nlength);
                    for (int j = 0; j < nlength; j++) {
                        char *temp = GetNthParamListI(handler, i - 1, j, &is_null);
                        if (!is_null) {
                            PyList_SetItem(pList, j, Py_BuildValue("f", atof(temp)));
                        } else {
                            PyList_SetItem(pList, j, Py_None);
                        }
                    }
                    PyTuple_SetItem(pArgs, i, pList);
                    break;
                }
                case TEXTARRAYOID:
                case VARCHARARRAYOID: {
                    int nlength = GetNthParamListLength(handler, i - 1);
                    if (nlength == -1) {
                        PyTuple_SetItem(pArgs, i, Py_None);
                        continue;
                    }
                    PyObject *pList = PyList_New(nlength);
                    for (int j = 0; j < nlength; j++) {
                        char *temp = GetNthParamListI(handler, i - 1, j, &is_null);
                        if (!is_null) {
                            PyList_SetItem(pList, j, Py_BuildValue("s", temp));
                        } else {
                            PyList_SetItem(pList, j, Py_None);
                        }
                    }
                    PyTuple_SetItem(pArgs, i, pList);
                    break;
                }
                case DATEOID:
                case TIMEOID:
                case TIMESTAMPOID:
                case TIMESTAMPTZOID: {
                    char *para = GetNthParam(handler, i - 1, &is_null);
                    if (!is_null) {
                        PyTuple_SetItem(pArgs, i, Py_BuildValue("s", (char *) para));
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_None);
                    }
                    break;
                }
                case BOOLOID: {
                    char *para = GetNthParam(handler, i - 1, &is_null);
                    if (is_null) {
                        PyTuple_SetItem(pArgs, i, Py_None);
                        break;
                    }
                    if (strcmp(para, "true") == 0) {
                        PyTuple_SetItem(pArgs, i, Py_True);
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_False);
                    }
                    break;
                }
                case BOOLARRAYOID: {
                    int nlength = GetNthParamListLength(handler, i - 1);
                    if (nlength == -1) {
                        PyTuple_SetItem(pArgs, i, Py_None);
                        break;
                    }
                    PyObject *pList = PyList_New(nlength);
                    for (int j = 0; j < nlength; j++) {
                        char *temp = GetNthParamListI(handler, i - 1, j, &is_null);
                        if (is_null) {
                            PyList_SetItem(pList, j, Py_None);
                            continue;
                        }
                        if (strcmp(temp, "true") == 0) {
                            PyList_SetItem(pList, j, Py_True);
                        } else {
                            PyList_SetItem(pList, j, Py_False);
                        }
                    }
                    PyTuple_SetItem(pArgs, i, pList);
                    break;
                }

                case BYTEAOID: {
                    int length;
                    char *para = GetNthBytesParam(handler, i - 1, &length, &is_null);
                    if (is_null) {
                        PyTuple_SetItem(pArgs, i, Py_None);
                        break;
                    } else {
                        PyTuple_SetItem(pArgs, i, Py_BuildValue("y#", para, length));
                    }
                    break;
                }

                default:
                    pythonParas->errorMsg = "Invalid input Params !";
                    PyGILState_Release(gstate);
                    pthread_exit(NULL);
                    return NULL;

                    PyTuple_SetItem(pArgs, i, Py_BuildValue("s", " Invalid Params !"));
                    break;
            }
        }
    }
    SetupFound(handler);

    pRes = PyObject_CallObject(pFunc, pArgs);
    PyObject *pErr = PyErr_Occurred();
    char *plrv_sc;
    int size;
    if (pErr == NULL) {
        plrv_so = PyObject_Str(pRes);
        if (meta.prorettype == BYTEAOID) {
            PyArg_Parse(pRes, "s#", &plrv_sc, &size);
            bool errOccured = false;
            char *err = get_python_traceback(&errOccured);
            if (errOccured) {
                pythonParas->errorMsg = err;
                PyGILState_Release(gstate);
                pthread_exit(NULL);
                return NULL;
            }
        } else {
            PyArg_Parse(plrv_so, "s", &plrv_sc);
            bool errOccured = false;
            char *err = get_python_traceback(&errOccured);
            if (errOccured) {
                pythonParas->errorMsg = err;
                PyGILState_Release(gstate);
                pthread_exit(NULL);
                return NULL;
            }
        }
    } else {
        bool errOccured = false;
        char *err = get_python_traceback(&errOccured);
        if (errOccured) {
            pythonParas->errorMsg = err;
            PyGILState_Release(gstate);
            pthread_exit(NULL);
            return NULL;
        }

    }
    bool isStr = true;
    if (!isStr) {
        char *query = palloc(12 + strlen(plrv_sc));
        strcat(query, "select E('");
        strcat(query, plrv_sc);
        strcat(query, "')");
        BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
        planIndex = BEPI_prepare_params(handler, query, 0);
        char *errorMsg = NULL;
        bool is_query_or_hasReturning = false;
        errorMsg = BepiExecPlan(handler, planIndex, &is_query_or_hasReturning);
        if (strlen(errorMsg) != 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg(errorMsg)));
        }
        errorMsg = BepiAssignResultToReturn(handler);
        if (strlen(errorMsg) != 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg(errorMsg)));
        }
    } else {
        if (pErr == NULL) {
            char *errorMsg = NULL;
            switch (meta.prorettype) {
                case BYTEAOID:
                    errorMsg = BepiAssignBytesToReturn(handler, plrv_sc, meta.prorettype, size);
                    if (errorMsg != NULL && strlen(errorMsg) != 0) {
                        pythonParas->errorMsg = errorMsg;
                    }
                    break;
                case BOOLARRAYOID:
                case FLOAT4ARRAYOID:
                case FLOAT8ARRAYOID:
                case TEXTARRAYOID:
                case VARCHARARRAYOID:
                case INT8ARRAYOID:
                case INT4ARRAYOID:
                case INT2ARRAYOID: {
                    if (!PyList_Check(pRes)) {
                        errorMsg = "The return from python is not a list ! ";
                        pythonParas->errorMsg = errorMsg;
                        break;
                    }
                    size_t length = PyList_Size(pRes);
                    char **results = palloc(sizeof(char *) * length);
                    int *sizes = palloc(sizeof(int) * length);
                    for (int i = 0; i < length; i++) {
                        PyObject *value = PyList_GetItem(pRes, i);
                        char *result;
                        int resultSize = 0;
                        if (value != NULL) {
                            PyObject *pstr = PyObject_Str(value);
                            PyArg_Parse(pstr, "s#", &result, &resultSize);
                            results[i] = result;
                            sizes[i] = resultSize;
                        } else {

                        }
                    }
                    errorMsg = BepiAssignArrayToReturn(handler, results, meta.prorettype, length, sizes);
                    if (errorMsg != NULL && strlen(errorMsg) != 0) {
                        pythonParas->errorMsg = errorMsg;
                    }
                    break;
                }
                default: {
                    char *typ = palloc(sizeof(char) * (strlen(pRes->ob_type->tp_name) + 1));
                    strcpy(typ, pRes->ob_type->tp_name);
                    errorMsg = BepiAssignStrToReturn(handler, plrv_sc, meta.prorettype, typ);
                    if (errorMsg != NULL && strlen(errorMsg) != 0) {
                        PyErr_SetString(PyExc_RuntimeError, errorMsg);
                        pythonParas->errorMsg = errorMsg;
                    }
                    break;
                }

            }
        }
        pfree(plrv_sc);
    }
    PyGILState_Release(gstate);
    pthread_exit(NULL);
    return NULL;
}
#endif

// function pointer for UDF C.
typedef void *(*FUNC_CALL_CLANG)(Handler_t, ...);

/* ----------
 * call_clang_32
 *
 * Call function of Language C UDF.
 * Support 32 params for now.
 * ----------
 */
static void *call_clang_32(Handler_t handle, FUNC_CALL_CLANG pFunc, int argNums, void *pArgs[]) {
    void (*void_func)() = NULL;
    if (argNums < 0 || argNums > 32) {
        return void_func;
    }
    switch (argNums) {
        case 0: {
            return pFunc(handle);
        }
        case 1: {
//            printf("test---------------------\n");
            return pFunc(handle, pArgs[0]);
        }
        case 2: {
            return pFunc(handle, pArgs[0], pArgs[1]);
        }
        case 3: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2]);
        }
        case 4: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3]);
        }
        case 5: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4]);
        }
        case 6: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5]);
        }
        case 7: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6]);
        }
        case 8: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7]);
        }
        case 9: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8]);
        }
        case 10: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9]);
        }
        case 11: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9], pArgs[10]);
        }
        case 12: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9], pArgs[10], pArgs[11]);
        }
        case 13: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9], pArgs[10], pArgs[11], pArgs[12]);
        }
        case 14: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9], pArgs[10], pArgs[11], pArgs[12], pArgs[13]);
        }
        case 15: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9], pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14]);
        }
        case 16: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9], pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15]);
        }
        case 17: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16]);
        }
        case 18: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17]);
        }
        case 19: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18]);
        }
        case 20: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19]);
        }
        case 21: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20]);
        }
        case 22: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21]);
        }
        case 23: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22]);
        }
        case 24: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23]);
        }
        case 25: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24]);
        }
        case 26: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25]);
        }
        case 27: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25], pArgs[26]);
        }
        case 28: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25], pArgs[26], pArgs[27]);
        }
        case 29: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25], pArgs[26], pArgs[27], pArgs[28]);
        }
        case 30: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25], pArgs[26], pArgs[27], pArgs[28],
                         pArgs[29]);
        }
        case 31: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25], pArgs[26], pArgs[27], pArgs[28],
                         pArgs[29], pArgs[30]);
        }
        case 32: {
            return pFunc(handle, pArgs[0], pArgs[1], pArgs[2], pArgs[3], pArgs[4], pArgs[5], pArgs[6], pArgs[7],
                         pArgs[8], pArgs[9],
                         pArgs[10], pArgs[11], pArgs[12], pArgs[13], pArgs[14], pArgs[15], pArgs[16], pArgs[17],
                         pArgs[18], pArgs[19], pArgs[20],
                         pArgs[21], pArgs[22], pArgs[23], pArgs[24], pArgs[25], pArgs[26], pArgs[27], pArgs[28],
                         pArgs[29], pArgs[30], pArgs[31]);
        }
    }
    return void_func;
}

/* ----------
 * plpgsql_clang_exec
 *
 * C UDF exec function.
 * ----------
 */
int plpgsql_clang_exec(Handler_t handler) {
    FormData_pg_proc proc_desc = SearchFunctionMetaCache(handler, PROC_CATCH_ID);
    char *file_path = proc_desc.file_path;
    char *file_name_full = proc_desc.file_name_full;
    char *file_path_full = palloc(1024);
    sprintf(file_path_full, "%s/%s", file_path, file_name_full);
    FUNC_CALL_CLANG pFunc;
    char *perr = NULL;

    // reset FuncHandler to NULL before open a .so file
    funcHandle = NULL;
    funcHandle = dlopen(file_path_full, RTLD_LAZY);
    if (funcHandle == NULL) {
        sprintf(handler.ErrMsg, "Open %s Failed.\n", file_name_full);
        dlclose(funcHandle);
        return -1;
    }
    char *func_name = proc_desc.proname.data;
    pFunc = dlsym(funcHandle, func_name);
    perr = dlerror();
    if (perr != NULL) {
        sprintf(handler.ErrMsg, "%s.\n", perr);
        dlclose(funcHandle);
        return -1;
    }

    if (proc_desc.pronargs > 32) {
        sprintf(handler.ErrMsg, "The number of arguments is greater than 32.\n");
        dlclose(funcHandle);
        return -1;
    }

    void *args[32];
    bool is_null = false;
    for (int i = 0; i < proc_desc.pronargs; i++) {
        switch (proc_desc.argtypes[i]) {
            case INT8OID:
            case INT4OID:
            case INT2OID: {
                int *param = palloc(sizeof(int));
                *param = atoi(GetNthParam(handler, i, &is_null));
                args[i] = param;
                break;
            }
            case BOOLOID: {
                int *param = palloc(sizeof(int));
                char *szParam = GetNthParam(handler, i, &is_null);
                if (strcmp(szParam, "true") == 0) {
                    *param = 1;
                } else {
                    *param = 0;
                }
                args[i] = param;
                break;
            }
            case FLOAT4OID:
            case FLOAT8OID: {
                float *param = palloc(sizeof(float));
                *param = atof(GetNthParam(handler, i, &is_null));
                args[i] = param;
                break;
            }
            case TEXTOID:
            case BYTEAOID:
            case VARCHAROID: {
                char *params = GetNthParam(handler, i, &is_null);
                char *pArg = palloc(strlen(params) * sizeof(char));
                strcpy(pArg, params);
                args[i] = pArg;
                break;
            }
            case INT8ARRAYOID:
            case INT4ARRAYOID:
            case INT2ARRAYOID: {
                int nlength = GetNthParamListLength(handler, i);
                int *pArg = palloc(nlength * sizeof(int));
                for (int j = 0; j < nlength; j++) {
                    char *temp = GetNthParamListI(handler, i, j, &is_null);
                    pArg[j] = atoi(temp);
                }
                BEPI_ARRAY_INT_PTR int_array = palloc(sizeof(BEPI_ARRAY_INT_PTR));
                int_array->vals = pArg;
                int_array->num = nlength;
                args[i] = int_array;
                break;
            }
            case FLOAT4ARRAYOID:
            case FLOAT8ARRAYOID: {
                int nlength = GetNthParamListLength(handler, i);
                float *pArg = palloc(nlength * sizeof(float));
                for (int j = 0; j < nlength; j++) {
                    char *temp = GetNthParamListI(handler, i, j, &is_null);
                    pArg[j] = atof(temp);
                }
                BEPI_ARRAY_FLOAT_PTR float_array = palloc(sizeof(BEPI_ARRAY_FLOAT_PTR));
                float_array->vals = pArg;
                float_array->num = nlength;
                args[i] = float_array;
                break;
            }
            case TEXTARRAYOID:
            case VARCHARARRAYOID: {
                int nlength = GetNthParamListLength(handler, i);
                char **pArg = palloc(nlength * sizeof(char *));
                for (int j = 0; j < nlength; j++) {
                    char *param = GetNthParamListI(handler, i, j, &is_null);
                    char *newParam = palloc(strlen(param) * sizeof(char));
                    strcpy(newParam, param);
                    pArg[j] = newParam;
                }
                BEPI_ARRAY_STRING_PTR string_array = palloc(sizeof(BEPI_ARRAY_STRING_PTR));
                string_array->vals = pArg;
                string_array->num = nlength;
                args[i] = string_array;
                break;
            }
            case BITOID: {
                int *param = palloc(sizeof(int));
                *param = atoi(GetNthParam(handler, i, &is_null));
                BEPI_BIT_PTR bit_var = palloc(sizeof(BEPI_BIT_PTR));
                bit_var->vals = (unsigned int) param[0];
                args[i] = bit_var;
                break;
            }
            case VARBITOID: {
                char *param = palloc(sizeof(char));
                param = GetNthParam(handler, i, &is_null);
                int length = strlen(param);
                BEPI_VARBIT_PTR varbit_var = palloc(sizeof(BEPI_VARBIT_PTR));
                varbit_var->num = length;
                varbit_var->vals = palloc(length * sizeof(int));
                for (int j = 0; j < length; j++) {
                    varbit_var->vals[j] = (unsigned int) (param[j]) - 48;
                    /*if ((varbit_var->vals[j] != 0)&&(varbit_var->vals[j] != 1)) {
                        sprintf(handler.ErrMsg, "Illegal VARBIT type \n");
                        return -1;
                    }*/
                }
                args[i] = varbit_var;
                break;
            }
            case DATEOID: {
                char *suffix = " 00:00:00+00:00";
                char str[128];
                char *param = palloc(sizeof(char));
                param = GetNthParam(handler, i, &is_null);
                sprintf(str, "%s%s", param, suffix);
                BEPI_TIMESTAMP *p = BEPI_str2ts(str);
                p->time_type = DATE;

                char *errorMsg;
                char *valstr;
                errorMsg = BepiCtime2str(handler, *p, &valstr);
                if (errorMsg != NULL && strlen(errorMsg) != 0) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(errorMsg)));

                }
                args[i] = p;
                break;
            }

            case TIMEOID: {
                char *prefix = "1900-01-01 ";
                char *suffix = "+00:00";
                char str[128];
                char *param = palloc(sizeof(char));
                param = GetNthParam(handler, i, &is_null);
                sprintf(str, "%s%s%s", prefix, param, suffix);
                BEPI_TIMESTAMP *p = BEPI_str2ts(str);
                p->time_type = TIME;
                char *errorMsg;
                char *valstr;
                errorMsg = BepiCtime2str(handler, *p, &valstr);
                if (errorMsg != NULL && strlen(errorMsg) != 0) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(errorMsg)));

                }
                args[i] = p;
                break;
            }
            case TIMESTAMPOID: {
                char *param = GetNthParam(handler, i, &is_null);
                BEPI_TIMESTAMP *p = BEPI_str2ts(param);
                p->time_type = TIMESTAMP;
                char *errorMsg;
                char *valstr;
                errorMsg = BepiCtime2str(handler, *p, &valstr);
                if (errorMsg != NULL && strlen(errorMsg) != 0) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(errorMsg)));
                }
                args[i] = p;
                break;

            }
            case TIMESTAMPTZOID: {
                char *param = GetNthParam(handler, i, &is_null);
                BEPI_TIMESTAMP *p = BEPI_str2ts(param);
                p->time_type = TIMESTAMPTZ;
                char *errorMsg;
                char *valstr;
                errorMsg = BepiCtime2str(handler, *p, &valstr);
                if (errorMsg != NULL && strlen(errorMsg) != 0) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(errorMsg)));

                }
                args[i] = p;
                break;

                break;
            }
            case NUMERICOID: {
                char *param = palloc(sizeof(char));
                param = GetNthParam(handler, i, &is_null);
                /*int precision = strlen(param);
                char *buf[2], *p;
                int n = 0;
                p = NULL;
                p = strtok(param,".");
                while(p)
                {
                    buf[n] = p;
                    ++i;
                    p = strtok(NULL,".");
                }
                int scale = strlen(buf[1]);*/
                numeric *numeric_var = TYPESnumeric_new();
                numeric_var = TYPESnumeric_from_asc(param, NULL);
                args[i] = numeric_var;
                break;
            }
            default:
                break;
        }
    }
    void *result = call_clang_32(handler, pFunc, proc_desc.pronargs, args);
    if (strcmp(handler.ErrMsg, "") != 0) {
        dlclose(funcHandle);
        return -1;
    }
    //return
    char res[1024];
    switch (proc_desc.prorettype) {
        case INT8OID:
        case INT4OID:
        case INT2OID: {
            sprintf(res, "%d", *(int *) result);
            break;
        }
        case BOOLOID: {
            if (*(int *) result == 0) {
                sprintf(res, "false");
            } else {
                sprintf(res, "true");
            }
            break;
        }
        case FLOAT4OID:
        case FLOAT8OID: {
            sprintf(res, "%f", *(float *) result);
            break;
        }
        case TEXTOID:
        case BYTEAOID:
        case VARCHAROID: {
            if (result != NULL) {
                sprintf(res, "%s", (char *) result);
            } else {
                sprintf(res, "%s", "");
            }
            break;
        }
        case INT8ARRAYOID:
        case INT4ARRAYOID:
        case INT2ARRAYOID: {
            BEPI_ARRAY_INT_PTR int_array = (BEPI_ARRAY_INT_PTR) result;
            sprintf(res, "%d", int_array->vals[0]);
            for (int i = 1; i < int_array->num; i++) {
                sprintf(res, "%s, %d", res, int_array->vals[i]);
            }
            break;
        }
        case FLOAT4ARRAYOID:
        case FLOAT8ARRAYOID: {
            BEPI_ARRAY_FLOAT_PTR float_array = (BEPI_ARRAY_FLOAT_PTR) result;
            sprintf(res, "%f", float_array->vals[0]);
            for (int i = 1; i < float_array->num; i++) {
                sprintf(res, "%s, %f", res, float_array->vals[i]);
            }
            break;
        }
        case TEXTARRAYOID:
        case VARCHARARRAYOID: {
            BEPI_ARRAY_STRING_PTR string_array = (BEPI_ARRAY_STRING_PTR) result;
            if (string_array != NULL) {
                if (string_array->vals != NULL) {
                    sprintf(res, "%s", string_array->vals[0]);
                    for (int i = 1; i < string_array->num; i++) {
                        sprintf(res, "%s,%s", res, string_array->vals[i]);
                    }
                }
                break;
            }

            case BITOID: {
                BEPI_BIT_PTR bit_var = (BEPI_BIT_PTR) result;
                if (bit_var != NULL) {
                    sprintf(res, "%d", bit_var->vals);
                }
                break;
            }
            case VARBITOID: {
                BEPI_VARBIT_PTR varbit_var = (BEPI_VARBIT_PTR) result;
                sprintf(res, "%d", varbit_var->vals[0]);
                for (int i = 1; i < varbit_var->num; i++) {
                    sprintf(res, "%s%d", res, varbit_var->vals[i]);
                }
                break;
            }
            case NUMERICOID: {
                numeric *numeric_res = TYPESnumeric_new();
                numeric_res = (numeric *) result;
                char *num;
                num = TYPESnumeric_to_asc(numeric_res, -1);
                sprintf(res, "%s", num);
                break;
            }
            default: {
                sprintf(res, "not support return value.");
                break;
            }
        }
    }

    perr = BepiAssignStrToReturn(handler, res, proc_desc.prorettype, "");
    if (strcmp(perr, "") != 0) {
        sprintf(handler.ErrMsg, "%s.\n", perr);
        dlclose(funcHandle);
        return -1;
    }

    dlclose(funcHandle);

    for (int i = 0; i < 32; i++) {
        if (args[i] != NULL) {
            pfree(args[i]);
        }
    }

    if (result != NULL) {
        pfree(result);
    }
    return 0;
}

Datum plpgsql_call_handler(FunctionCallInfo fcinfo, Handler_t handler) {
    fcinfo->flinfo = &fcinfo->sflinfo;
    bool nonatomic;
    PLpgSQL_function *func;
    PLpgSQL_execstate *save_cur_estate;
    Datum retval;
    FormData_pg_proc meta = SearchFunctionMetaCache(handler, PROC_CATCH_ID);
    /* Find or compile the function */
    // pthread_mutex_lock(&mutex);
    func = plpgsql_compile(handler, fcinfo, false);
    // pthread_mutex_unlock(&mutex);

    /* Must save and restore prior value of cur_estate */
    save_cur_estate = func->cur_estate;

    /* Mark the function as busy, so it can't be deleted from under us */
    func->use_count++;
    PG_TRY();
            {
                char *py_lang = "plpython";
                char *c_lang = "plclang";

                if (strcmp(py_lang, meta.language) == 0) {
#ifndef __APPLE__
                    if (!Py_IsInitialized()) //检测是否初始化成功
                    {
                        Py_Initialize();
                        if (!Py_IsInitialized()) {
                            ereport(ERROR,
                                    (errmsg("Py_Initialized() failed!\n")));
                        }
                    }
                    retval = 1;
                    paras *pythonParas = palloc(sizeof(paras));
                    pythonParas->fcinfo = fcinfo;
                    pythonParas->handler = handler;
                    pythonParas->nonatomic = &nonatomic;
                    pythonParas->func = func;
                    pythonParas->errorMsg = NULL;
                    /*pythonParas->retval = &retval;*/
                    /*plpgsql_python_script((void*)pythonParas);*/
                    pid_t thread_id = gettid();
                    // 开启线程执行plpython函数
                    if (!PyEval_ThreadsInitialized()) {
                        PyEval_InitThreads();
                    }
                    PyEval_ReleaseThread(PyThreadState_Get());
                    pthread_t tid = 1;
                    pthread_create(&tid, NULL, plpgsql_python_script, (void *) pythonParas);
                    pthread_join(tid, NULL);
                    PyGILState_Ensure();
                    if (pythonParas->errorMsg != NULL) {
                        if (strlen(pythonParas->errorMsg) != 0)
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                            errmsg(pythonParas->errorMsg)));
                    }
#endif
                } else if (strcmp(c_lang, meta.language) == 0) {
                    if (handler.ErrMsg == NULL) {
                        handler.ErrMsg = (char *) palloc(128 * sizeof(char));
                    }
                    retval = plpgsql_clang_exec(handler);
                    if (retval == -1) {
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(handler.ErrMsg)));
                        pfree(handler.ErrMsg);
                    }
                } else {
                    retval = plpgsql_exec_function(handler, func, fcinfo, NULL, !nonatomic);
                }
            }
        PG_CATCH();
            {
                /* Decrement use-count, restore cur_estate, and propagate error */
                func->use_count--;
                func->cur_estate = save_cur_estate;
                errfinish(0);
            }
    PG_END_TRY();
    func->use_count--;
    func->cur_estate = save_cur_estate;
    return retval;
}

/* ----------
 * plpgsql_inline_handler
 *
 * Called by PostgreSQL to execute an anonymous code block
 * ----------
 */

Datum
plpgsql_inline_handler(PG_FUNCTION_ARGS) {
    LOCAL_FCINFO(fake_fcinfo, 0);
    InlineCodeBlock *codeblock = castNode(InlineCodeBlock, DatumGetPointer(PG_GETARG_DATUM(0)));
    PLpgSQL_function *func;
    FmgrInfo flinfo;
    Datum retval;
    /* Compile the anonymous code block */
    func = plpgsql_compile_inline(codeblock->source_text);

    /* Mark the function as busy, just pro forma */
    func->use_count++;

    /*
     * Set up a fake fcinfo with just enough info to satisfy
     * plpgsql_exec_function().  In particular note that this sets things up
     * with no arguments passed.
     */
    MemSet(fake_fcinfo, 0, SizeForFunctionCallInfo(0));
    MemSet(&flinfo, 0, sizeof(flinfo));
    fake_fcinfo->flinfo = &flinfo;
    flinfo.fn_oid = InvalidOid;
    flinfo.fn_mcxt = CurrentMemoryContext;

    /* And run the function */
    PG_TRY();
            {
                /*retval = plpgsql_exec_function(func, fake_fcinfo, simple_eval_estate, codeblock->atomic);*/
            }
        PG_CATCH();
            {
                /*
                 * We need to clean up what would otherwise be long-lived resources
                 * accumulated by the failed DO block, principally cached plans for
                 * statements (which can be flushed with plpgsql_free_function_memory)
                 * and execution trees for simple expressions, which are in the
                 * private EState.
                 *
                 * Before releasing the private EState, we must clean up any
                 * simple_econtext_stack entries pointing into it, which we can do by
                 * invoking the subxact callback.  (It will be called again later if
                 * some outer control level does a subtransaction abort, but no harm
                 * is done.)  We cheat a bit knowing that plpgsql_subxact_cb does not
                 * pay attention to its parentSubid argument.
                 */

                /* Clean up the private EState */
                //FreeExecutorState(simple_eval_estate);

                /* Function should now have no remaining use-counts ... */
                func->use_count--;
                Assert(func->use_count == 0);

                /* ... so we can free subsidiary storage */
                plpgsql_free_function_memory(func);

                /* And propagate the error */
                /*PG_RE_THROW();*/
            }
    PG_END_TRY();

    /* Clean up the private EState */
    //FreeExecutorState(simple_eval_estate);

    /* Function should now have no remaining use-counts ... */
    func->use_count--;
    Assert(func->use_count == 0);

    /* ... so we can free subsidiary storage */
    plpgsql_free_function_memory(func);

    /*
     * Disconnect from SPI manager
     */
    //	if ((rc = SPI_finish()) != SPI_OK_FINISH)
    //		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc));

    return retval;
}

/* ----------
 * plpgsql_validator
 *
 * This function attempts to validate a PL/pgSQL function at
 * CREATE FUNCTION time.
 * ----------
 */

Datum plpgsql_validator(FunctionCallInfo fcinfo, Handler_t handler) {
    //Go代码里赋值给了结构体sflinfo，pg源码使用指针fcinfo，这里取地址
    fcinfo->flinfo = &fcinfo->sflinfo;

    Oid funcOid = fcinfo->flinfo->fn_oid;
    Form_pg_proc proc;
    int numargs;
    int i;

    FormData_pg_proc procReal;
    procReal = SearchFunctionMetaCache(handler, PROC_CATCH_ID);
    proc = &procReal;

    numargs = proc->pronargs;
    for (i = 0; i < numargs; i++) {
        // 不支持的类型在这里添加
        // 直接报错
    }

    /* Postpone body checks if !check_function_bodies */
    //if (check_function_bodies)
    if (true) {
        LOCAL_FCINFO(fake_fcinfo, 0);
        FmgrInfo flinfo;
        MemSet(fake_fcinfo, 0, SizeForFunctionCallInfo(0));
        MemSet(&flinfo, 0, sizeof(flinfo));
        fake_fcinfo->flinfo = &flinfo;
        flinfo.fn_oid = funcOid;
        flinfo.fn_mcxt = CurrentMemoryContext;

        /* Test-compile the function */
        plpgsql_compile(handler, fake_fcinfo, true);
    }
    return (Datum) 0;
}

