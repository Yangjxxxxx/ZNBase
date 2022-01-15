#include "./foo.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

BEPI_INT_PTR TestTime(
        Handler_t handler,
        BEPI_TIMESTAMP *time,
        BEPI_TIMESTAMP *Date,
        BEPI_TIMESTAMP *ts,
        BEPI_TIMESTAMP *tstz
) {
    pr(time);
    pr(Date);
    pr(ts);
    pr(tstz);
    BEPI_INT_PTR res = (BEPI_INT_PTR)malloc(sizeof(int));
    *res = 0;
    return res ;
}

void pr(BEPI_TIMESTAMP *t) {
    switch (t->time_type) {
        case DATE: {
            printf("DATE        :");
            time_t  tt = mktime(&t->tm);
            printf("  %s  %ld\n",BEPI_ts2str("%Y-%m-%d %H:%M:%S %Z",t),tt);
            break;
        }
        case TIME: {
            printf("TIME        :");
            time_t  tt = mktime(&t->tm);
            printf("  %s  %ld\n",BEPI_ts2str("%Y-%m-%d %H:%M:%S %Z",t),tt);
            break;

        }
        case TIMESTAMP: {
            printf("TIMESTAMP   :");
            time_t  tt = mktime(&t->tm);
            printf("  %s  %ld\n",BEPI_ts2str("%Y-%m-%d %H:%M:%S %Z",t),tt);
            break;

        }
        case TIMESTAMPTZ: {
            printf("TIMESTAMPTZ :");
            time_t  tt = mktime(&t->tm);
            printf("  %s  %ld\n",BEPI_ts2str("%Y-%m-%d %H:%M:%S %Z",t),tt);
            break;

        }
    }
}

// test prepare and exec plan
BEPI_INT_PTR TestExecPlan(
        Handler_t handler) {
    BEPIPlanIndex index = 0;
    char *sql = "Create Table test(a int, b float, c string)";
    index = BEPI_prepare(handler, sql);
    int ret = BEPI_execute_plan(handler, index);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

// test BEPI_exec
BEPI_INT_PTR TestExec1(
        Handler_t handler) {
    char *sql = "insert into test(a, b, c) values(26, 10.1, '101')";
    int ret = BEPI_exec(handler, sql);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

// test BEPI_execute
BEPI_INT_PTR InsertAnyValues(
        Handler_t handler,
        BEPI_STRING_PTR table_name,
        BEPI_INT_PTR times,
        BEPI_ARRAY_INT_PTR a,
        BEPI_ARRAY_FLOAT_PTR b,
        BEPI_ARRAY_STRING_PTR c) {
    int insertTimes = *times;
    int ret = 0;
    for (int i = 0; i < insertTimes; i++) {
        char sql[100];
        sprintf(sql, "insert into %s(a, b, c) values(%d, %f, '%s')", table_name, a->vals[i], b->vals[i], c->vals[i]);
        BEPI_execute(handler, sql);
    }

    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

//test args
BEPI_ARRAY_INT_PTR ReturnArray1(
        Handler_t handler,
        BEPI_INT_PTR arg1,
        BEPI_FLOAT_PTR arg2,
        BEPI_STRING_PTR arg3
) {
    BEPI_ARRAY_INT_PTR int_array = (BEPI_ARRAY_INT_PTR) BEPI_palloc(sizeof(BEPI_ARRAY_INT_PTR));
    int argNums = BEPI_getargcount(handler);
    int_array->vals = (BEPI_INT_PTR) BEPI_palloc(sizeof(int) * argNums);
    for (int i = 0; i < argNums; i++) {
        int_array->vals[i] = BEPI_getargtypeid(handler, i);
    }
    int_array->num = argNums;
    return int_array;
}

BEPI_ARRAY_FLOAT_PTR ReturnArray2(
        Handler_t handler) {
    BEPI_ARRAY_FLOAT_PTR float_array = (BEPI_ARRAY_FLOAT_PTR) BEPI_palloc(sizeof(BEPI_ARRAY_FLOAT_PTR));
    float_array->vals = (BEPI_FLOAT_PTR) BEPI_palloc(sizeof(float) * 10);
    for (int i = 0; i < 10; i++) {
        float_array->vals[i] = 99.9 * i;
    }
    float_array->num = 10;
    return float_array;
}

BEPI_ARRAY_STRING_PTR ReturnArray3(
        Handler_t handler) {
    BEPI_ARRAY_STRING_PTR string_array = (BEPI_ARRAY_STRING_PTR) BEPI_palloc(sizeof(BEPI_ARRAY_STRING_PTR));
    string_array->vals = BEPI_palloc(sizeof(char *) * 10);
    for (int i = 0; i < 10; i++) {
        char *newParam = (BEPI_STRING_PTR) BEPI_palloc(64 * sizeof(char));
        sprintf(newParam, "string array test: %d", i);
        string_array->vals[i] = newParam;
    }
    string_array->num = 10;
    return string_array;
}

BEPI_STRING_PTR TestCursor(
        Handler_t handler,
        BEPI_STRING_PTR cursor_name,
        BEPI_STRING_PTR query_sql
) {
    BEPI_STRING_PTR errMsg = (BEPI_STRING_PTR) BEPI_palloc(128 * sizeof(char));
    //DECLARE CURSOR
    cursorArg_name_type *curArgNameType;
    curArgNameType->name = "a";
    curArgNameType->type_name = "int";
    curArgNameType->typoid = 23;
    BEPI_cursor_declare(handler, cursor_name, query_sql, curArgNameType, 1);
    //OPEN
    BEPI_cursor_open_with_paramlist(handler, cursor_name, NULL, "(1)", true);
    //find cursor
    CursorDesc cur_desc = BEPI_cursor_find(handler, cursor_name, &errMsg);
    //MOVE
    BEPI_cursor_move(handler, cursor_name, FETCH_FORWARD, 2); //forward
    BEPI_cursor_move(handler, cursor_name, FETCH_BACKWARD, 1); //backward
    //FETCH
    BEPI_cursor_fetch(handler, cursor_name, FETCH_FORWARD, 3); //forward
    //CLOSE
    BEPI_cursor_close(handler, cursor_name, &errMsg);
    BEPI_pfree(errMsg);
    return "";
}

BEPI_INT_PTR TestSelect2(Handler_t handler) {
    BEPIPlanIndex index = 0;
    char *sql = "select eee from udftest.uschema.ctest ";
    index = BEPI_prepare(handler, sql);
    int ret = BEPI_execp(handler, index);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_INT_PTR TestReturn(Handler_t handler) {
    char *sql1 = "select e from testcudf";
    int ret1 = BEPI_exec(handler, sql1);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret1;
    return ptr;
}

BEPI_STRING_PTR TestCursor2(Handler_t handler, BEPI_STRING_PTR cursor_name, BEPI_STRING_PTR query_sql) {
    cursorArg_name_type curArgNameType;
    curArgNameType.name = (BEPI_STRING_PTR) BEPI_palloc(2 * sizeof(char));
    curArgNameType.name = "a";
    curArgNameType.type_name = (BEPI_STRING_PTR) BEPI_palloc(4 * sizeof(char));
    curArgNameType.type_name = "int";
    curArgNameType.typoid = 23;
    BEPI_cursor_declare(handler, cursor_name, query_sql, &curArgNameType, 1);
    int ret = BEPI_cursor_open_with_paramlist(handler, cursor_name, NULL, "1", true);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_INT_PTR TestTwoTcount(Handler_t handler) {
    char *sql1 = "select * from testcudf";
    int ret1 = BEPI_exec(handler, sql1);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret1;
    return ptr;
}

BEPI_INT_PTR TestBitExecPlan(
        Handler_t handler) {
    BEPIPlanIndex index = 0;
    char *sql = "Create Table bit(a bit)";
    index = BEPI_prepare(handler, sql);
    int ret = BEPI_execute_plan(handler, index);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_INT_PTR InsertBitValues(
        Handler_t handler,
        BEPI_BIT_PTR a) {
    char *sql = (BEPI_STRING_PTR) BEPI_palloc(64 * sizeof(char));
    sprintf(sql, "insert into bit values(B'%d')", a->vals);
    int ret = BEPI_exec(handler, sql);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_INT_PTR TestVarbitExecPlan(
        Handler_t handler) {
    BEPIPlanIndex index = 0;
    char *sql = "Create Table testvarbit(a varbit)";
    index = BEPI_prepare(handler, sql);
    int ret = BEPI_execute_plan(handler, index);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_BIT_PTR ReturnBit(Handler_t handler) {
    BEPI_BIT_PTR bit_var = (BEPI_BIT_PTR) BEPI_palloc(sizeof(BEPI_BIT_PTR));
    memset(bit_var, 0, sizeof(BEPI_BIT_PTR));
    bit_var->vals = 1;
    return bit_var;
}

BEPI_INT_PTR InsertVarbitValues(
        Handler_t handler,
        BEPI_VARBIT_PTR a) {
    char *varbitstr = (BEPI_STRING_PTR) BEPI_palloc(32 * sizeof(char));
    memset(varbitstr, 0, 32 * sizeof(char));
    for (int i = 0; i < a->num; i++) {
        sprintf(varbitstr, "%s%d", varbitstr, a->vals[i]);
    }
    char *sql = (BEPI_STRING_PTR) BEPI_palloc(64 * sizeof(char));
    memset(sql, 0, 64 * sizeof(char));
    sprintf(sql, "insert into testvarbit values(varbit'%s')", varbitstr);
    int ret = BEPI_exec(handler, sql);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_VARBIT_PTR ReturnVarbit(Handler_t handler) {
    BEPI_VARBIT_PTR varbit_var = (BEPI_VARBIT_PTR) BEPI_palloc(sizeof(BEPI_VARBIT_PTR));
    memset(varbit_var, 0, sizeof(BEPI_VARBIT_PTR));
    varbit_var->num = 10;
    varbit_var->vals = BEPI_palloc(sizeof(unsigned int) * (varbit_var->num));
    memset(varbit_var->vals, 0, sizeof(unsigned int) * (varbit_var->num));
    for (int i = 0; i < varbit_var->num; i++) {
        if (i % 2 == 0) {
            varbit_var->vals[i] = 0;
        } else {
            varbit_var->vals[i] = 1;
        }
    }
    return varbit_var;
}

BEPI_INT_PTR TestExecDecimal(Handler_t handler, BEPI_STRING_PTR num) {
    numeric *a;
    printf("test---\n");
    a = TYPESnumeric_new();
    a = TYPESnumeric_from_asc(num, NULL);
    printf("res----------\n");
    char *sql = (BEPI_STRING_PTR) BEPI_palloc(64 * sizeof(char));
    memset(sql, 0, 64 * sizeof(char));
    sprintf(sql, "Create Table testdecimal(a DECIMAL(8,3))");
    BEPIPlanIndex index = 0;
    index = BEPI_prepare(handler, sql);
    int ret = BEPI_execute_plan(handler, index);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR) BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

BEPI_INT_PTR InsertDecimal(Handler_t handler, numeric *a) {
    double num;
    int i = TYPESnumeric_to_double(a, &num);
    char *sql = (BEPI_STRING_PTR)BEPI_palloc(64*sizeof(char));
    memset(sql,0,64*sizeof(char));
    sprintf(sql,"Insert into testdecimal values('%.3lf'::decimal)",num);
    int ret = BEPI_exec(handler, sql);
    BEPI_INT_PTR ptr = (BEPI_INT_PTR)BEPI_palloc(sizeof(int));
    *ptr = ret;
    return ptr;
}

numeric * ReturnDecimal(Handler_t handler, BEPI_STRING_PTR num) {
    numeric *a = TYPESnumeric_new();
    a = TYPESnumeric_from_asc(num, NULL);
    return a;
}