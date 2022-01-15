#include "plpgsql.h"
#include <time.h>
#include <sys/time.h>

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
const char *elog_header = "This file is znbase plsql's log file, used for debug and check plsql's compile and execute step.\n";
// 唯一的日志文件句柄
static ELOG_File *efile = NULL;
extern bool plsql_sigjum_inited;

#define LEVELMAX  11
const char *levelString[LEVELMAX] = {
    "DEBUG5",
    "DEBUG4",
    "DEBUG3",
    "DEBUG2",
    "DEBUG1",
    "LOG",
    "LOG_SERVER_ONLY",
    "INFO",
    "NOTICE",
    "WARNING",
    "ERROR",
};

/*
* This macro handles expansion of a format string and associated parameters;
* it's common code for errmsg(), errdetail(), etc.  Must be called inside
* a routine that is declared like "const char *fmt, ..." and has an edata
* pointer set up.  The message is assigned to edata->targetfield, or
* appended to it if appendval is true.  The message is subject to translation
* if translateit is true.
*
* Note: we pstrdup the buffer rather than just transferring its storage
* to the edata field because the buffer might be considerably larger than
* really necessary.
*/
#define EVALUATE_MESSAGE(domain, targetfield, appendval, translateit)   \
{ \
    StringInfoData  buf; \
    /* Internationalize the error format string */ \
    if ((translateit) && !in_error_recursion_trouble()) \
        fmt = dgettext((domain), fmt);                \
    initStringInfo(&buf); \
    if ((appendval) && edata->targetfield) { \
        appendStringInfoString(&buf, edata->targetfield); \
        appendStringInfoChar(&buf, '\n'); \
    } \
    /* Generate actual output --- have to use appendStringInfoVA */ \
    for (;;) \
    { \
        va_list     args; \
        int         needed; \
        errno = edata->saved_errno; \
        va_start(args, fmt); \
        needed = appendStringInfoVA(&buf, fmt, args); \
        va_end(args); \
        if (needed == 0) \
            break; \
        enlargeStringInfo(&buf, needed); \
    } \
    /* Save the completed message into the stack item */ \
    if (edata->targetfield) \
        pfree(edata->targetfield); \
    edata->targetfield = pstrdup(buf.data); \
    pfree(buf.data); \
}

// 先构造一个全局的errorData 对象，用于保存报错信息
// 考虑如何处理并发报错？？？
extern sigjmp_buf plsql_sigjum_buf;

// errstart 进行结构的填充，如果级别> ERROR 进行错误输出
bool errstart(int elevel, const char *filename, int lineno,
              const char *funcname, const char *domain){
    PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
    ErrorData *errorData = plsql_data->errData;
    errorData->elevel = elevel;
    if (elevel >= ERROR) {
        errorData->output_to_client = true;
        errorData->output_to_server = false;
    }
    errorData->filename = filename;
    errorData->lineno = lineno;
    errorData->funcname = funcname;
    errorData->domain = domain == NULL ? domain :"plsql";
    if (elevel >= ERROR)
        return true;
    else 
        // 不跳转了
        return false;
}

void errfinish(int dummy,...){
    if (plsql_sigjum_inited) {
        // 首先判断是否能够处理异常
        PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
        siglongjmp(*(plsql_data->plsql_sigjum_buf[plsql_data->index]), 1);
    }
    else {
        // unlikely happen
        PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
        ErrorData *errorData = plsql_data->errData;
        elog(ERROR, "Not set long jump, directly output in log file:\n");
        elog(ERROR, errorData->message);
    }
}
//函数名：  elogfile_create
//作者：    jiye
//日期：    2020-06-03
//功能：    创建znbase-plsql 的日志文件，并打开。
//输入参数：elog_path : 日志文件的路径.
//返回值:   类型(bool)
//          创建是否成功。
bool elogfile_create(const char *elog_path) {
    if (efile == NULL) {
        if (elog_path == NULL) {
            return false; 
        }
        efile = fopen(elog_path, "a+");
        if (efile == NULL) {
            return false;
        }
        // 文件创建成功，写入文件头
        time_t curTime;
        curTime = time(NULL);
        struct tm *pTime;
        pTime = localtime(&curTime);
        char timebuf[100];
        strftime(timebuf,100, "time: %r, %a %b %d, %Y", pTime);
        char fileHeader[1024] = {0};
        sprintf(fileHeader, "%sOpen at: %s.\n", elog_header, timebuf);
        int len = strlen(fileHeader);
        fwrite(fileHeader, len, 1, efile); 
        int errNum = ferror(efile);
        if ( errNum != 0 ) {
            clearerr(efile);
            return false; 
        } else {
            if (fflush(efile)) {
                return false;
            } else return true;
        }
    } else {
        elog(INFO, "elog file has already been created and open!");
        return true;
    }
}
//函数名：  elog_output
//作者：    jiye
//日期：    2020-06-03
//功能：    输出到ZNBase 的日志文件中
//输入参数：elevel : 日志级别.
//          filename: 日志相关的文件名。
//          lineno: 行号。
//          funcname: 函数名。
//          fmt: 输出的日志信息format字符串。
void elog_output(int elevel, const char *filename, int lineno, const char *funcname, const char *fmt,...) {
    #ifdef __APPLE__
    return ;
    #endif
    StringInfoData  buf;
    initStringInfo(&buf);
    for (;;) 
    { 
        va_list     args; 
        int         needed; 
        va_start(args, fmt); 
        needed = appendStringInfoVA(&buf, fmt, args); 
        va_end(args); 
        if (needed == 0) 
            break; 
        enlargeStringInfo(&buf, needed); 
    } 
    time_t curTime;
    curTime = time(NULL);
    struct tm* pTime;
    pTime = localtime(&curTime);
    char timebuf[100];
    strftime(timebuf, 100, "time: %r, %a %b %d, %Y", pTime);
    // 直接输出日志信息
    char *fileBuffer = NULL;
    int len = buf.len + 512; // buf's and 512 bytes
    fileBuffer = (char*) palloc(len);
    sprintf(fileBuffer, "[ZNBase-PLSQL][Time :%s][Level :%s][Filename:%s, Lineno: %d, Funcname: %s]: %s\n", timebuf, levelString[elevel - STARTLEVEL], filename, lineno, funcname, buf.data );
    len = strlen(fileBuffer);
    if (efile == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("elog file is not opened or created!")));
    }
    fwrite(fileBuffer, len, 1, efile);
    int errNum = ferror(efile);
    if (errNum != 0) {
        clearerr(efile);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("elog file write failed!")));
    } else {
        if (fflush(efile)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("elog file flush failed!")));
        }
    }
    pfree(fileBuffer);
    pfree(buf.data);
}

int	errmsg(const char *fmt,...){

    PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
    ErrorData *errorData = plsql_data->errData;
    if (errorData) {
        StringInfoData  buf;
        initStringInfo(&buf);
        for (;;) 
        { 
            va_list     args; 
            int         needed; 
            va_start(args, fmt); 
            needed = appendStringInfoVA(&buf, fmt, args); 
            va_end(args); 
            if (needed == 0) 
                break; 
            enlargeStringInfo(&buf, needed); 
        } 
        char sqlerrcode[5] = {'X','X','0','O','O'};
        // len must five to get sqlerrcode
        strncpy(sqlerrcode, buf.data, 5);
        int code = MAKE_SQLSTATE_WITHCHARS(sqlerrcode);
        // 前六位是错误码
        errorData->message = buf.data;
        errorData->message_len = buf.len;
        errorData->sqlerrcode = code;
    }
    return 1;
}

//函数名：  CopyErrorData
//作者：    jiye
//日期：    2020-11-12
//功能：    从PLSQLDATA 里面拷贝错误日志。
ErrorData *CopyErrorData(void) {
    PLSQLData *plsqldata = PLSQL_GetCurrentThread_Data();
    ErrorData *newedata;
    /* Copy the struct itself */
    newedata = (ErrorData *) palloc(sizeof(ErrorData));
    memcpy(newedata, plsqldata->errData, sizeof(ErrorData));

    /* Make copies of separately-allocated fields */
    if (newedata->message)
        newedata->message = pstrdup(newedata->message);
    if (newedata->detail)
        newedata->detail = pstrdup(newedata->detail);
    if (newedata->hint)
        newedata->hint = pstrdup(newedata->hint);
    if (newedata->context)
        newedata->context = pstrdup(newedata->context);
    if (newedata->schema_name)
        newedata->schema_name = pstrdup(newedata->schema_name);
    if (newedata->table_name)
        newedata->table_name = pstrdup(newedata->table_name);
    if (newedata->column_name)
        newedata->column_name = pstrdup(newedata->column_name);
    if (newedata->datatype_name)
        newedata->datatype_name = pstrdup(newedata->datatype_name);
    if (newedata->constraint_name)
        newedata->constraint_name = pstrdup(newedata->constraint_name);
    if (newedata->internalquery)
        newedata->internalquery = pstrdup(newedata->internalquery);
    /* Use the calling context for string allocation */
    newedata->assoc_context = CurrentMemoryContext;
    return newedata;
}
//函数名：  errcode 
//作者：    jiye
//日期：    2020-11-12
//功能：    设置报错的错误码。
int	errcode(int sqlerrcode){
    PLSQLData *plsqldata = PLSQL_GetCurrentThread_Data();
    ErrorData *errorData = plsqldata->errData;
    if (errorData) {
        errorData->sqlerrcode = sqlerrcode;
    }
    return 0;
}

int errdetail(const char *fmt,...){
    return 1;
}

int errhint(const char *fmt,...){
    return 1;
}

int errposition(int cursorpos){
    return 1;
}

char *unpack_sql_state(int sql_state)
{
    static char buf[12];
    int         i;
    for (i = 0; i < 5; i++)
    {
        buf[i] = PGUNSIXBIT(sql_state);
        sql_state >>= 6;
    }

    buf[i] = '\0';
    return buf;
}
/*
* set_errdata_field --- set an ErrorData string field
*/
static void
set_errdata_field(MemoryContextData *cxt, char **ptr, const char *str)
{
    Assert(*ptr == NULL);
    *ptr = MemoryContextStrdup(cxt, str);
}
/*
* err_generic_string -- used to set individual ErrorData string fields
* identified by PG_DIAG_xxx codes.
*
* This intentionally only supports fields that don't use localized strings,
* so that there are no translation considerations.
*
* Most potential callers should not use this directly, but instead prefer
* higher-level abstractions, such as errtablecol() (see relcache.c).
*/
int
err_generic_string(int field, const char *str)
{
    PLSQLData *plsqldata = PLSQL_GetCurrentThread_Data();
    ErrorData  *edata = plsqldata->errData;
    /* we don't bother incrementing recursion_depth */
    switch (field)
    {
      case PG_DIAG_SCHEMA_NAME:
          set_errdata_field(edata->assoc_context, &edata->schema_name, str);
          break;
      case PG_DIAG_TABLE_NAME:
          set_errdata_field(edata->assoc_context, &edata->table_name, str);
          break;
      case PG_DIAG_COLUMN_NAME:
          set_errdata_field(edata->assoc_context, &edata->column_name, str);
          break;
      case PG_DIAG_DATATYPE_NAME:
          set_errdata_field(edata->assoc_context, &edata->datatype_name, str);
          break;
      case PG_DIAG_CONSTRAINT_NAME:
          set_errdata_field(edata->assoc_context, &edata->constraint_name, str);
          break;
      default:
          elog(ERROR, "unsupported ErrorData field id: %d", field);
          break;
    }

    return 0;                   /* return value does not matter */
}
/*
* errmsg_internal --- add a primary error message text to the current error
*
* This is exactly like errmsg() except that strings passed to errmsg_internal
* are not translated, and are customarily left out of the
* internationalization message dictionary.  This should be used for "can't
* happen" cases that are probably not worth spending translation effort on.
* We also use this for certain cases where we *must* not try to translate
* the message because the translation would fail and result in infinite
* error recursion.
*/
int
errmsg_internal(const char *fmt,...)
{
    PLSQLData *plsqldata = PLSQL_GetCurrentThread_Data();
    ErrorData  *edata = plsqldata->errData;
    MemoryContext oldcontext;
    // do not report domain in this version 
    oldcontext = MemoryContextSwitchTo(edata->assoc_context);
    EVALUATE_MESSAGE(edata->domain, message, false, false);
    MemoryContextSwitchTo(oldcontext);
    /*recursion_depth--;*/
    return 0;                   /* return value does not matter */
}
/*
* errdetail_internal --- add a detail error message text to the current error
*
* This is exactly like errdetail() except that strings passed to
* errdetail_internal are not translated, and are customarily left out of the
* internationalization message dictionary.  This should be used for detail
* messages that seem not worth translating for one reason or another
* (typically, that they don't seem to be useful to average users).
*/
int
errdetail_internal(const char *fmt,...)
{
    PLSQLData *plsqldata = PLSQL_GetCurrentThread_Data();
    ErrorData  *edata = plsqldata->errData;
    MemoryContext oldcontext;
    // do not report domai in this version 
    /*recursion_depth++;*/
    /*CHECK_STACK_DEPTH();*/
    oldcontext = MemoryContextSwitchTo(edata->assoc_context);
    EVALUATE_MESSAGE(edata->domai, detail, false, false);
    MemoryContextSwitchTo(oldcontext);
    /*recursion_depth--;*/
    return 0;                   /* return value does not matter */
}
int
geterrcode(void)
{
    return 1;
}

/*
 * geterrposition --- return the currently set error position (0 if none)
 *
 * This is only intended for use in error callback subroutines, since there
 * is no other place outside elog.c where the concept is meaningful.
 */
int
geterrposition(void)
{
    return 1;
}

/*
 * getinternalerrposition --- same for internal error position
 *
 * This is only intended for use in error callback subroutines, since there
 * is no other place outside elog.c where the concept is meaningful.
 */
int
getinternalerrposition(void)
{
    return 1;
}

int
internalerrposition(int cursorpos)
{
    return 0;                   /* return value does not matter */
}

