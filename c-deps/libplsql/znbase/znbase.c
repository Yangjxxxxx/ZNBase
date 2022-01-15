#include "plpgsql.h"
#include <signal.h>
#include <execinfo.h>
#include <unistd.h>

#define SEGVSTR     "unExpected Error: segv error !!"
#define FPESTR      "unExpected Error: fpe error !!"
#define BUSSTR      "unExpected Error: bus error !!"
#define ILLSTR      "unExpected Error: illegal instruction error !!"
#define OTHERSTR    "unExpected Error: others error!!"


const char *plsql_error_prefix = "ZNBase PLSQL ERMSG: %s";
const char *plsql_error_oom = "out of memory when execute this function!";
const char *plsql_error_elogfailed = "plsql elog file create or open failed!";
const char *plsql_error_hashmapfailed = "plsql hash map created failed!";
char * error_path = NULL;
/* common internal error */
const char *plsql_error_common_internal = "plsql internal error happen, please contact with developer of znbase!";
bool plsql_inited = false;
bool plsql_sigjum_inited = false;
const char *log_path_format = "%s/znbase-plsql.log";

char *sigerrMsg;

#define DEBUG_UDR true

// 全局变量池
// 此MAP属于全局唯一MAP, 从TopMemoryContex 上申请， 不需要释放。
hashmap_map *Map = NULL;


// 异常处理函数
void SignalHandler(int sigNum) {
    if (DEBUG_UDR){
	    #define STACK_SIZE 32
	    void *trace[STACK_SIZE];
        char error_stack_msg[20000];
	    size_t size = backtrace(trace, STACK_SIZE);
	    char **symbols_stack ;
        symbols_stack = backtrace_symbols(trace,size);
        
        elog_output(0,error_path,46,"--------------------------------------------------------------------\n","");
        size_t i = 0;
	    for(; i<size; i++)
	    {
            char msg [8192];
            sprintf(msg,"%s\n",symbols_stack[i]);
		    strcat(error_stack_msg,msg);
	    }
        elog_output(0,error_path,54,"stack :\n",error_stack_msg);
        elog_output(0,error_path,55,"--------------------------------------------------------------------\n","");
    }
    if (sigNum == SIGSEGV) {
        sigerrMsg = (char*)SEGVSTR;
    } else if (sigNum == SIGFPE) {
        sigerrMsg = (char*)FPESTR;
    } else if (sigNum == SIGBUS) {
        sigerrMsg = (char*)BUSSTR;
    } else if (sigNum == SIGILL) {
        sigerrMsg = (char*)ILLSTR;
    } else {
        sigerrMsg = (char*)OTHERSTR;
    }
    PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
    siglongjmp(*plsql_data->plsql_sigjum_buf[plsql_data->index], 1);
}

// 异常捕获函数2
void SignalAll() {
    signal(SIGSEGV, SignalHandler);
    signal(SIGFPE, SignalHandler);
    signal(SIGBUS, SignalHandler);
    signal(SIGILL, SignalHandler);
}

//函数名：  PLSQL_external_for_znbase
//作者：    jiye
//日期：    2020-06-02
//功能：    PLSQL 面向ZNBase 的唯一接口，
//          1. 命令分流。
//          2. 初始化。
//          3. 报错控制。
//输入参数：handler : UDR runtime params.
//          action_type: 命令类型。
//          fcinfo: 函数调用相关的结构体。
//          error_message: 报错详细信息, 报错数组长为1024byte, 该数组需要在GO中申请内存。
//返回值：  类型(bool)
//          返回执行成功与否。
bool PLSQL_external_for_znbase(Handler_t handler, PLSQLACTION action_type,  char *error_message, Pointer_vec vec, char *log_path,FunctionCallInfoBaseDataForGO fcinfo_base_cgo)
{
    FunctionCallInfoBaseData *fcinfo_base = (FunctionCallInfoBaseData *)&fcinfo_base_cgo;
    handler.pointer = vec.pointer;
    handler.ErrMsg = error_message;
    // 设置异常捕获与处理函数
    // if (!DEBUG_UDR){
        SignalAll();
    // }
    if (!plsql_inited)
    {
        // 进行报错跳转定义
        MemoryContextInit();
        Map = hashmap_new();
        if (Map == NULL)
        {
            sprintf(error_message, plsql_error_prefix, plsql_error_hashmapfailed);
            elog_output(0,error_path,48,"create error",error_message);
            return false;
        }
        int pathLen = strlen(log_path) + 32; // 默认路径长度+32bytes
        char *path = (char *)palloc0(pathLen);
        sprintf(path, log_path_format, log_path);
        error_path = path;
        if (elogfile_create(path) == false)
        {
            sprintf(error_message, plsql_error_prefix, plsql_error_elogfailed);
            elog_output(0,error_path,48,"create error",error_message);
            return false;
        }
        plsql_inited = true;
    }
    PLSQL_init_resource(handler);
    PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
    sigjmp_buf*  plsql_sigjum_buf = (sigjmp_buf*)palloc0(sizeof(sigjmp_buf));
    plsql_data->index++;
    plsql_data->plsql_sigjum_buf[plsql_data->index] = plsql_sigjum_buf;
    // 初始化顺序
    // 0. 跳转点初始化。1. 内存。 2. 全局变量池。3. 日志文件创建。
    if (sigsetjmp(*plsql_data->plsql_sigjum_buf[plsql_data->index], 1) != 0)
    {
        if (funcHandle) {
            dlclose(funcHandle);
        }
        plsql_data->index--;

        char *err = BepiTryToCloseAllUnClosedCursor(handler);
        if (strlen(err) != 0) {
            char *errtmp = palloc(strlen(err) + 50);
            sprintf(error_message, plsql_error_prefix, sprintf(errtmp, "error happen when close all unclosed cursors: %s", err));
            PLSQL_cleanup_resource();
            pfree(errtmp);
            return false;
        }
        if (sigerrMsg != NULL) {
            sprintf(error_message, plsql_error_prefix, sigerrMsg);
            sigerrMsg = NULL;
            return false;
        }
        // 执行失败，返回znbase false
        PLSQLData *plsql_data = PLSQL_GetCurrentThread_Data();
        if (plsql_data == NULL || plsql_data->errData == NULL)
        {
            sprintf(error_message, plsql_error_prefix, plsql_error_common_internal);
            elog_output(0,error_path,48,"no data",error_message);
            return false;
        }
        if (plsql_data->error_oom_happen)
        {
            // 本次操作发生了oom
            sprintf(error_message, plsql_error_prefix, plsql_error_oom);
            elog_output(0,error_path,48,"oom",error_message);
            if (plsql_data->index == 0 ){
                PLSQL_cleanup_resource();
            }
            return false;
        }
        // 取出errorData
        ErrorData *errorData = plsql_data->errData;
        sprintf(error_message, plsql_error_prefix, errorData->message);
        if (errorData->message_len >= MAX_ERROR_MSG_LEN)
        {
            // 如果超长则，直接截断报错
            error_message[MAX_ERROR_MSG_LEN - 1] = '\0';
        }
        elog_output(0,error_path,48,"message long",error_message);
        if (plsql_data->index == 0 ){
            PLSQL_cleanup_resource();
        }
        return false;
    }
    plsql_sigjum_inited = true;
    handler.rc = 0 ;
    switch (action_type)
    {
    case PLSQL_CALL_FUNCTION:
        plpgsql_call_handler(fcinfo_base, handler);
        break;
    case PLSQL_VALIDATOR:
        plpgsql_validator(fcinfo_base, handler);
        break;
    default:
        ereport(ERROR, (errmsg("plsql not suport this action!")));
        break;
    }
    plsql_data->index --;
    if (plsql_data->index == 0 ){
        PLSQL_cleanup_resource();
    }
    return true;
}

//函数名：  PLSQL_init_resource
//作者：    jiye
//日期：    2020-06-04
//输入参数：handler : UDR runtime params.
//功能：    初始化PLSQL 本次操作所需要的资源。
//return 是否是第一次初始化
bool PLSQL_init_resource(Handler_t handler)
{
    //从TopMemoryContex 新建本thread 的内存上下文，
    //之后凡是和全局变量相关的操作都需要在Thread 上下文上操作。
    MemoryContext thread_ctx, old_ctx;
     unsigned long long int  thread_id = BepiGetGoroutineID();
    PLSQLData *plsql_data = NULL;
    const char *ctxFormat = "PLSQL-THREAD-%llu";
    char hashKey[64] = {0};
    sprintf(hashKey, ctxFormat, thread_id);
    thread_ctx = AllocSetContextCreateInternal(TopMemoryContext,
                                               hashKey,
                                               ALLOCSET_DEFAULT_SIZES);
    old_ctx = MemoryContextSwitchTo(thread_ctx);
    plsql_data = (PLSQLData *)palloc0(sizeof(PLSQLData));

    char* keyname = (char *)palloc0(strlen(hashKey) + 1);
    plsql_data->KeyName  = keyname;
    sprintf(plsql_data->KeyName, "%s", hashKey);
    plsql_data->errData = (ErrorData *)palloc0(sizeof(ErrorData));
    plsql_data->errData->assoc_context = AllocSetContextCreateInternal(thread_ctx, 
                                            keyname, ALLOCSET_DEFAULT_SIZES);
    // 初始化errData的context
    plsql_data->ThreadMemoryContext = thread_ctx;
    plsql_data->handler = handler;
    // pthread_mutex_lock(&mutex);
    int error = hashmap_get(Map,plsql_data->KeyName,(any_t)plsql_data);
    // pthread_mutex_unlock(&mutex);
    if (error == MAP_OK){
        return false;
    }
    plsql_data->KeyName  = keyname;
    // pthread_mutex_lock(&mutex);
    error = hashmap_put(Map, plsql_data->KeyName, plsql_data);
    // pthread_mutex_unlock(&mutex);
    if (error != MAP_OK)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("plsql hash map add error!")));
    }
    old_ctx = MemoryContextSwitchTo(old_ctx);
    return true;
}

//函数名：  PLSQL_cleanup_resource
//作者：    jiye
//日期：    2020-06-04
//功能：    释放PLSQL 本次操作所需要的资源。
void PLSQL_cleanup_resource()
{
    PLSQLData *p = PLSQL_GetCurrentThread_Data();
    MemoryContext ctx = p->ThreadMemoryContext;
    int error = hashmap_remove(Map, p->KeyName);
    if (error != MAP_OK)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("plsql hash map remove error!")));
    }
    if (MemoryContextIsEmpty(ctx))
    {
        MemoryContextDelete(ctx);
    }
}
//函数名：  PLSQL_GetCurrentThread_Data
//作者：    jiye
//日期：    2020-06-04
//功能：    获取当前thread 下的全局data。
//返回值:   类型(PLSQLData*)
//          当前thread 对应得PLSQLData.
//          如果没有返回NULL。
PLSQLData *PLSQL_GetCurrentThread_Data()
{
    PLSQLData *plsql_data = NULL;
    const char *ctxFormat = "PLSQL-THREAD-%llu";
    char hashKey[64] = {0};
    unsigned long long int  thread_id = BepiGetGoroutineID();
    sprintf(hashKey, ctxFormat, thread_id);
    if (Map == NULL)
    {
        return plsql_data;

    }
    // pthread_mutex_lock(&mutex);
    int error = hashmap_get(Map, hashKey, (void **)&plsql_data);
    // pthread_mutex_unlock(&mutex);
    if (error != MAP_OK || plsql_data == NULL)
    {
        return NULL;
        errfinish(0);
    }
    return plsql_data;
}

//函数名：  PLSQL_GetCurrentUdrRunParamAddr
//作者：    zh
//日期：    2020-07-31
//功能：    获取当前thread 下的全局data中的UDRRunParam's address
//返回值:   类型(int*)
//          当前thread 对应得UDRRunParam's address
//          如果没有返回NULL。
UDRVarFind PLSQL_GetCurrentUdrRunParamAddr(char *varName, int placeIdx) {
    PLSQLData *plsqlData = PLSQL_GetCurrentThread_Data();
    bool found = false;
    unsigned long res = BepiFindVarByName(plsqlData->handler, varName, placeIdx, &found);
    char *err = palloc(ERRSIZE);
    if (!found) {
        sprintf(err, "can not find a variable with name: %s or placeIdx: %d", varName, placeIdx);
    }
    UDRVarFind result;
    result.paramAddr = res;
    result.err = palloc(ERRSIZE);
    MemSet(result.err, 0, ERRSIZE);
    strcpy(result.err, err);
    return result;
}

UDRVarDatumFind PLSQL_GetCurrentUdrRunParamDatumStr(char *varName, int placeIdx) {
    PLSQLData *plsqlData = PLSQL_GetCurrentThread_Data();
    bool found = false;
    char *res = BepiFindVarDatumByNameToString(plsqlData->handler, varName, placeIdx, &found);
    char *err = palloc(ERRSIZE);
    if (!found) {
        sprintf(err, "can not find a variable with name: %s or placeIdx: %d", varName, placeIdx);
    }
    UDRVarDatumFind result;
    result.paramDatumStr = *res;
    result.err = palloc(ERRSIZE);
    MemSet(result.err, 0, ERRSIZE);
    strcpy(result.err, err);
    return result;
}

char *format_type_be(Oid type_oid)
{
    return "unknown type";
}

// Type Related interface
ExpandedRecordHeader *
make_expanded_record_from_typeid(Oid type_id, int32 typmod,
                                 MemoryContext parentcontext)
{
    return NULL;
}

// SPI 伪接口
int SPI_connect(void) { return 0; }
int SPI_connect_ext(int options) { return 0; }
int SPI_finish(void) { return 0; }
int SPI_execute(const char *src, bool read_only, long tcount) { return 0; }
int SPI_execute_plan(SPIPlanPtr plan, Datum *Values, const char *Nulls,
                     bool read_only, long tcount) { return 0; }
//函数名： BEPI_execute_plan_with_paramlist
//作者：    jiye
//日期：    2020-05-29
//功能：    BEPI 接口，给定一个已经生成的计划，在znbase 上执行。
//输入参数：handler : UDR runtime params.
//          planIndex: znbase 计划的索引。
//          params: TO BE ADD.
//          read_Only: TO BE ADD.
//          tcount: TO BE ADD.
//          is_query_or_hasReturning: 判断语句是否为查询或者带returning的语句
//返回值：  类型(int)
//          返回执行该计划的结果。
int BEPI_execute_plan_with_paramlist(Handler_t handler, BEPIPlanIndex planIndex,
                                     ParamListInfo params,
                                     bool read_only, long tcount, bool *is_query_or_hasReturning)
{
    char *errorMsg = NULL;
    errorMsg = BepiExecPlan(handler, planIndex, is_query_or_hasReturning);
    // TODO: 增加报错体系
    if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
         ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg(errorMsg)));
        return -1;
    }
    return SPI_OK_SELECT;
}


int BEPI_execute(Handler_t handler, const char *src)
{
    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *errorMsg = (char *)palloc(ERRSIZE);
    char *sql = (char *)palloc(sizeof(char)*strlen(src));
    strcpy(sql,src);
    memset(errorMsg, 0, ERRSIZE);
    errorMsg = BepiPreparePlan(handler, sql, 0, &planIndex);
    if (strlen(errorMsg) != 0)
    {
        ereport(ERROR,(errmsg("error int BEPIPrepare: \"%s\"",errorMsg)));
        return -1;
    }
    bool is_query_or_hasReturning = false;
    errorMsg = BepiExecPlan(handler, planIndex, &is_query_or_hasReturning);
    if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
         ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg(errorMsg)));
        return -1;
    }
    return 0;
}

int BEPI_exec(Handler_t handler, const char *src)
{
    return BEPI_execute(handler, src);
}

int BEPI_execute_plan(Handler_t handler, BEPIPlanIndex planIndex)
{
    char *errorMsg = NULL;
    bool is_query_or_hasReturning = false;
        errorMsg = BepiExecPlan(handler, planIndex, &is_query_or_hasReturning);
        if (errorMsg != NULL && strlen(errorMsg) != 0)
        {
             ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg(errorMsg)));
            return -1;
        }
    return 0;
}

int BEPI_execp(Handler_t handler, BEPIPlanIndex planIndex)
{
    return BEPI_execute_plan(handler, planIndex);
}

char **BEPI_execute_plan_with_returnrows(Handler_t handler, BEPIPlanIndex planIndex,
                                     ParamListInfo params, Oid *oid,
                                     bool read_only, long tcount)
{
    char *errorMsg = NULL;
    bool is_query_or_hasReturning = false;
    errorMsg = BepiExecPlan(handler, planIndex, &is_query_or_hasReturning);
    if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg(errorMsg)));
    }
	char **attrNames;
    int colNum = 0;
    int * attrSizes;
    char **retStr = BepiGetReturn(handler, &oid, &tcount, &attrNames, &colNum,&attrSizes);
    return retStr;
}

int BEPI_TXN_commit(Handler_t handler){
    char *errorMsg = "Unsupported transactions.";
        if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg(errorMsg)));
        return -1;
    }
    return 0;
}

int BEPI_TXN_rollback(Handler_t handler){
    char *errorMsg = "Unsupported transactions.";
        if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
         ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg(errorMsg)));
        return -1;
    }
    return 0;
}
//函数名： BEPI_exec_eval_expr
//作者：    liuqinghua
//日期：    2020-06-13
//功能：    BEPI 接口，表达式求值
//输入参数：handler : UDR runtime params
//        planIndex: znbase 计划的索引
//        result: 求值结果
//返回值：  类型(bool)
//          返回是否成功
bool
BEPI_exec_eval_expr(
        Handler_t handler,
        BEPIPlanIndex planIndex,
        char *varName,
        int dno,
        Datum *result,
        Oid *retType,
        bool in_using,
        bool *isNull)
{
    char *errorMsg = NULL;
    VarRes var_res_desc;
    errorMsg = BepiExecEvalExprAndStoreInResult(handler, planIndex, &var_res_desc, isNull);
    if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
        ereport(ERROR,(errmsg("ERROR: \"%s\"",errorMsg)));
    }
    if (*isNull) {
        return false;
    }

    uint type = var_res_desc.typ;
    *retType = type;
    char* valStr = var_res_desc.data;
    if (in_using) {
        type = TEXTOID;
        *retType = type;
    }
	switch (type) {
		case INT2OID:
		case INT4OID:
		case INT8OID: {
			int64 value = atoi(valStr);
			*result = Int64GetDatum(value);
			return true;
		}
		case FLOAT4OID: {
			float value = atof(valStr);
			*result = Float4GetDatum((float4)value);
			return true;
		}
		case NUMERICOID:
		case FLOAT8OID: {
			double value = atof(valStr);
			*result = Float8GetDatum((float8)value);
			return true;
		}
		case BOOLOID: {
			*result = BoolGetDatum(strcmp(valStr, "true") == 0);
			return true;
		}
		case VARCHAROID:
		case TEXTOID: {
			*result = CStringGetDatum(valStr);
			return true;
		}
		case RECORDOID: {
		    return true;
		}
		default:
			return *isNull;

	}
}

int SPI_exec(const char *src, long tcount) { return 0; }
int SPI_execp(SPIPlanPtr plan, Datum *Values, const char *Nulls,
              long tcount) { return 0; }
int SPI_execute_snapshot(SPIPlanPtr plan,
                         Datum *Values, const char *Nulls,
                         Snapshot snapshot,
                         Snapshot crosscheck_snapshot,
                         bool read_only, bool fire_triggers, long tcount) { return 0; }
int SPI_execute_with_args(const char *src,
                          int nargs, Oid *argtypes,
                          Datum *Values, const char *Nulls,
                          bool read_only, long tcount) { return 0; }
SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes) { return 0; }
SPIPlanPtr SPI_prepare_cursor(const char *src, int nargs, Oid *argtypes,
                              int cursorOptions) { return 0; }
//函数名：  BEPI_prepare_params
//作者：    jiye
//日期：    2020-05-27
//功能：    BEPI 接口，用于在ZNBase 生成某个查询的逻辑计划，并保存到UDR 运行参数之中。
//输入参数：handler : UDR runtime params.
//          src: 查询，或表达式, 或sql 语句的字符串。
//          cursorOptions: 游标选项。
//返回值：  类型(BEPIPlanIndex)
//          返回ZNBase UDR运行参数中关于该表达式的计划缓存的索引。
BEPIPlanIndex BEPI_prepare_params(Handler_t handler, const char *src, int cursorOptions)
{
    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *errorMsg = (char *)palloc(ERRSIZE);
    char *sql = (char *)palloc(sizeof(char)*strlen(src));
    strcpy(sql,src);
    memset(errorMsg, 0, ERRSIZE);
    errorMsg = BepiPreparePlan(handler, sql, cursorOptions, &planIndex);
    if (strlen(errorMsg) != 0)
    {
        ereport(ERROR,(errmsg(errorMsg)));
    }
    return planIndex;
}

BEPIPlanIndex BEPI_prepare(Handler_t handler, const char *src)
{
    return BEPI_prepare_params(handler, src, 0);
}

int SPI_keepplan(SPIPlanPtr plan) { return 0; }
SPIPlanPtr SPI_saveplan(SPIPlanPtr plan) { return 0; }
int SPI_freeplan(SPIPlanPtr plan) { return 0; }

Oid SPI_getargtypeid(SPIPlanPtr plan, int argIndex) { return 0; }
int SPI_getargcount(SPIPlanPtr plan) { return 0; }
//函数名：  BEPI_getargtypeid
//作者：    taoxudong
//日期：    2020-08-25
//功能：    BEPI 接口，用于返回DRDB UDR Params参数Oid。
//输入参数：handler : UDR runtime params.
//        argIndex : int
//返回值：  类型(int)
//          返回DRDB UDR运行参数Oid。
uint32 BEPI_getargtypeid(Handler_t handler, int argIndex) {
    uint32 id;
    id = BepiGetArgTypeID(handler, argIndex);
    return id;
}

//函数名：  BEPI_getargcount
//作者：    taoxudong
//日期：    2020-08-25
//功能：    BEPI 接口，用于返回DRDB UDR Params参数数量。
//输入参数：handler : UDR runtime params.
//返回值：  类型(int)
//          返回DRDB UDR运行参数数量。
int BEPI_getargcount(Handler_t handler) {
    int count;
    count = BepiGetArgCount(handler);
    return count;
}
bool SPI_is_cursor_plan(SPIPlanPtr plan) { return 0; }
bool SPI_plan_is_valid(SPIPlanPtr plan) { return 0; }
const char *SPI_result_code_string(int code) { return 0; }

List *SPI_plan_get_plan_sources(SPIPlanPtr plan) { return 0; }
CachedPlan *SPI_plan_get_cached_plan(SPIPlanPtr plan) { return 0; }

HeapTuple SPI_copytuple(HeapTuple tuple) { return 0; }
HeapTupleHeader SPI_returntuple(HeapTuple tuple, TupleDesc tupdesc) { return 0; }
HeapTuple SPI_modifytuple(Relation rel, HeapTuple tuple, int natts,
                          int *attnum, Datum *Values, const char *Nulls) { return 0; }
int SPI_fnumber(TupleDesc tupdesc, const char *fname) { return 0; }
char *SPI_fname(TupleDesc tupdesc, int fnumber) { return 0; }
char *SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber) { return 0; }
Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull) { return 0; }
char *SPI_gettype(TupleDesc tupdesc, int fnumber) { return 0; }
Oid SPI_gettypeid(TupleDesc tupdesc, int fnumber) { return 0; }
char *SPI_getrelname(Relation rel) { return 0; }
char *SPI_getnspname(Relation rel) { return 0; }
void *SPI_palloc(Size size) { return (void *)0; }
void *SPI_repalloc(void *pointer, Size size) { return (void *)0; }
void SPI_pfree(void *pointer) { return; }
Datum SPI_datumTransfer(Datum value, bool typByVal, int typLen) { return 0; }
void SPI_freetuple(HeapTuple pointer) { return; }
void SPI_freetuptable(SPITupleTable *tuptable) { return; }

Portal SPI_cursor_open(const char *name, SPIPlanPtr plan,
                       Datum *Values, const char *Nulls, bool read_only) { return 0; }
Portal SPI_cursor_open_with_args(const char *name,
                                 const char *src,
                                 int nargs, Oid *argtypes,
                                 Datum *Values, const char *Nulls,
                                 bool read_only, int cursorOptions) { return 0; }
Portal SPI_cursor_open_with_paramlist(const char *name, SPIPlanPtr plan,
                                      ParamListInfo params, bool read_only) { return 0; }

//函数名：  BEPI_result_code_string
//作者：    taoxudong
//日期：    2020-08-27
//功能：    BEPI 接口，用于将任何BEPI返回码转换为字符串.
//输入参数：code : int.
//返回值：  类型(char)
//          返回错误或成功信息。
const char *BEPI_result_code_string(int code) {
    static char buf[64];

   switch (code)
   {
      case SPI_ERROR_CONNECT:
         return "SPI_ERROR_CONNECT";
      case SPI_ERROR_COPY:
         return "SPI_ERROR_COPY";
      case SPI_ERROR_OPUNKNOWN:
         return "SPI_ERROR_OPUNKNOWN";
      case SPI_ERROR_UNCONNECTED:
         return "SPI_ERROR_UNCONNECTED";
      case SPI_ERROR_ARGUMENT:
         return "SPI_ERROR_ARGUMENT";
      case SPI_ERROR_PARAM:
         return "SPI_ERROR_PARAM";
      case SPI_ERROR_TRANSACTION:
         return "SPI_ERROR_TRANSACTION";
      case SPI_ERROR_NOATTRIBUTE:
         return "SPI_ERROR_NOATTRIBUTE";
      case SPI_ERROR_NOOUTFUNC:
         return "SPI_ERROR_NOOUTFUNC";
      case SPI_ERROR_TYPUNKNOWN:
         return "SPI_ERROR_TYPUNKNOWN";
      case SPI_ERROR_REL_DUPLICATE:
         return "SPI_ERROR_REL_DUPLICATE";
      case SPI_ERROR_REL_NOT_FOUND:
         return "SPI_ERROR_REL_NOT_FOUND";
      case SPI_OK_CONNECT:
         return "SPI_OK_CONNECT";
      case SPI_OK_FINISH:
         return "SPI_OK_FINISH";
      case SPI_OK_FETCH:
         return "SPI_OK_FETCH";
      case SPI_OK_UTILITY:
         return "SPI_OK_UTILITY";
      case SPI_OK_SELECT:
         return "SPI_OK_SELECT";
      case SPI_OK_SELINTO:
         return "SPI_OK_SELINTO";
      case SPI_OK_INSERT:
         return "SPI_OK_INSERT";
      case SPI_OK_DELETE:
         return "SPI_OK_DELETE";
      case SPI_OK_UPDATE:
         return "SPI_OK_UPDATE";
      case SPI_OK_CURSOR:
         return "SPI_OK_CURSOR";
      case SPI_OK_INSERT_RETURNING:
         return "SPI_OK_INSERT_RETURNING";
      case SPI_OK_DELETE_RETURNING:
         return "SPI_OK_DELETE_RETURNING";
      case SPI_OK_UPDATE_RETURNING:
         return "SPI_OK_UPDATE_RETURNING";
      case SPI_OK_REWRITTEN:
         return "SPI_OK_REWRITTEN";
      case SPI_OK_REL_REGISTER:
         return "SPI_OK_REL_REGISTER";
      case SPI_OK_REL_UNREGISTER:
         return "SPI_OK_REL_UNREGISTER";
   }
   /* Unrecognized code ... return something useful ... */
   sprintf(buf, "Unrecognized SPI code %d", code);
   return buf;
 }


//函数名：  BEPI_cursor_declare
//作者：    zhanghao
//日期：    2020-06-22
//功能：    BEPI接口，用于声明cursor。
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          query: cursor 对应的query语句
//          curArg: cursor 的参数
//          arg_num: 参数数量
//          errmsg: 错误信息
//返回值：  int(open成功返回1，错误返回0)
//
int BEPI_cursor_declare(Handler_t handler, const char *name, char *query, cursorArg_name_type *curArg, int arg_num)
{
    char *errorMsg = (char *)palloc(ERRSIZE);
    memset(errorMsg, 0, ERRSIZE);
    char *arg_str = palloc(1024);
    memset(arg_str, 0, 1024);
    int i;

    if (arg_num > 0) {
        strcat(arg_str, "(");
        for (i = 0; i < arg_num; i++) {
            if (i != 0) {
                strcat(arg_str, ", ");
            }
            strcat(arg_str, (*(curArg+i)).name);
            strcat(arg_str, " ");
            strcat(arg_str, (*(curArg+i)).type_name);
        }
        strcat(arg_str, ")");
    }
    char *char_name = (char *)palloc(sizeof(char)*strlen(name));
    strcpy(char_name,name);
	int state = BepiCursorDeclare(handler, char_name, query, arg_str, &errorMsg);
    if (state == 0)
    {
        ereport(ERROR,
                (errmsg("error int BEPI_cursor_declare: \"%s\"",
                        errorMsg)));
    }
    pfree(errorMsg);
    return 0;
}


//函数名：  BEPI_cursor_open_with_paramlist
//作者：    zhanghao
//日期：    2020-06-19
//功能：    BEPI接口，用于在ZNBase打开cursor。
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          params: 参数实际
//返回值：  int(open成功返回1，失败直接报错)
//          或许应该返回相应的cursor信息（待考虑）
//          至少包含name，plan。
int BEPI_cursor_open_with_paramlist(Handler_t handler, const char *name, ParamListInfo params, char *arg_str, bool read_only)
{
    char *query;
    if (arg_str != NULL) {
        query = (char *)palloc(5 + strlen(name) + strlen(arg_str) + 3);
    } else {
        query = (char *)palloc(5 + strlen(name) + 1);
    }
    memset(query, 0, 6 + strlen(name));
    memcpy(query, "open ", 5);
    strcat(query, name);
    if (arg_str != NULL) {
        strcat(query, "(");
        strcat(query, arg_str);
        strcat(query, ")");
    }

    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    planIndex = BEPI_prepare_params(handler, query, CURSOR_OPT_OPEN);
    bool is_query_or_hasReturning = false;
    char *errorMsg = BepiExecPlan(handler, planIndex, &is_query_or_hasReturning);
    // 报错
    if (errorMsg != NULL && strlen(errorMsg) != 0)
    {
        ereport(ERROR,
                (errmsg("error happen when open cursor: \"%s\"",
                        errorMsg)));
    }
    char * char_name = (char *)palloc(sizeof(char)*strlen(name));
    strcpy(char_name,name);
    errorMsg = BepiAssignCursorOpen(handler, char_name);
    pfree(query);
    pfree(errorMsg);
    return 0;
}
Portal SPI_cursor_find(const char *name) { return 0; }

//函数名：  BEPI_cursor_find
//作者：    zhanghao
//日期：    2020-06-13
//功能：    BEPI 接口，用于在ZNBase 找到name所对应的curosr的描述符，并所谓返回值返回。
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          errmsg: 错误信息
//返回值：  类型(cursorDesc)
//          至少包含name，plan,。
CursorDesc BEPI_cursor_find(Handler_t handler, const char *name, char **errmg)
{
    char * char_name = (char*)palloc(sizeof(char)*sizeof(name));
    strcpy(char_name,name);
	unsigned long cdp = BepiCursorFind(handler, char_name, errmg);
	CursorDesc cursorDesc = *(CursorDescPtr)cdp;
	return cursorDesc;
}
void SPI_cursor_fetch(Portal portal, bool forward, long count) { return; }

//函数名：  BEPI_cursor_fetch
//作者：    zhanghao
//日期：    2020-07-30
//功能：    BEPI 接口，用于在ZNBase把cursor的行内容保存到UDRRunParams.CursorResult中
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          forward: 是否为forward, 如果false则说明backward
//
//          errmsg: 错误信息
//返回值：  类型(cursorDesc)
//          至少包含name，plan,。
/*bool BEPI_cursor_fetch(Handler_t handler, char *name, bool forward, long count)
{
        char *errmg = (char *)palloc(ERRSIZE);
        memset(errmg, 0, ERRSIZE);
        bool found = BepiCursorFetch(handler, name, forward, count, &errmg);
        if (strlen(errmg) != 0) {
            ereport(ERROR,
                    (errmsg("error happen when cursor fetch: %s", errmg)));
        }
        pfree(errmg);
        return found;
}*/

bool BEPI_cursor_fetch(Handler_t handler, char *name, FetchDirection direction, long count)
{
    char *errmg = (char *)palloc(ERRSIZE);
    memset(errmg, 0, ERRSIZE);
    bool found = BepiCursorFetch(handler, name, direction, count, &errmg);
    if (strlen(errmg) != 0) {
        ereport(ERROR,
                (errmsg("error happen when cursor fetch: %s", errmg)));
    }
    pfree(errmg);
    return found;
}

void SPI_cursor_move(Portal portal, bool forward, long count) { return; }

//函数名：  BEPI_cursor_move
//作者：    zhanghao
//日期：    2020-07-27
//功能：    BEPI 接口，用于移动cursor的位置
//
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          direction: move的方向
//          long:  move的距离
//
//返回值：  类型(cursorDesc)
//          至少包含name，plan,。
// void BEPI_cursor_move(Handler_t handler, char *name, bool forward, long count)
// {
//     char *errmg = (char *)palloc(ERRSIZE);
//     memset(errmg, 0, ERRSIZE);
//     bool res = BepiCursorMove(handler, name, forward, count, &errmg);
//     if (errmg != NULL && strlen(errmg) != 0) {
//         ereport(ERROR,
//                 (errmsg("%s", errmg)));
//     }
//     pfree(errmg);
// }

void BEPI_cursor_move(Handler_t handler, char *name, FetchDirection direction, long count)
{
    char *errmg = (char *)palloc(ERRSIZE);
    memset(errmg, 0, ERRSIZE);
    BepiCursorMove(handler, name, direction, count, &errmg);
    if (errmg != NULL && strlen(errmg) != 0) {
        ereport(ERROR,
                (errmsg("%s", errmg)));
    }
    pfree(errmg);
}

void SPI_scroll_cursor_fetch(Portal portal, FetchDirection direction, long count) { return; }
void SPI_scroll_cursor_move(Portal portal, FetchDirection direction, long count) { return; }
void SPI_cursor_close(Portal portal) { return; }

//函数名：  BEPI_cursor_close
//作者：    zhanghao
//日期：    2020-06-16
//功能：    BEPI 接口，用于在ZNBase 找到name所对应的curosr的描述符，根据desc的id等，通过engine，清除数据以达到close的目的。
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          errmsg: 错误信息
//返回值：  bool（1 代表close成功， 0 代表失败）
void BEPI_cursor_close(Handler_t handler, char *name, char *errmsg)
{
   BepiCursorClose(handler, name, &errmsg);
}


//函数名：  BEPI_castValue_toType
//作者：    zhanghao
//日期：    2020-06-16
//功能：    BEPI 接口
//输入参数：  Handler_t: param 参数
//          name : cursor name
//          errmsg: 错误信息
//返回值：  bool（1 代表close成功， 0 代表失败）
char *BEPI_castValue_toType(Handler_t handler, char *valStr, char **resStr, int srcOid, int castOid)
{
	char *err = NULL;
	err = BepiCastValueToType(handler, valStr, resStr, srcOid, castOid);
	if (err != NULL && strcmp(err, "") != 0) {
		ereport(ERROR, (errmsg("%s", err)));
	}
	return *resStr;
}

void *
BEPI_palloc(Size size)
{
	return palloc(size);
}

void
BEPI_pfree(void *pointer)
{
	pfree(pointer);
}

int SPI_register_relation(EphemeralNamedRelation enr) { return 0; }
int SPI_unregister_relation(const char *name) { return 0; }
int SPI_register_trigger_data(TriggerData *tdata) { return 0; }

void SPI_start_transaction(void) { return; }
void SPI_commit(void) { return; }

void SPI_commit_and_chain(void) { return; }
void SPI_rollback(void) { return; }
void SPI_rollback_and_chain(void) { return; }

void SPICleanup(void) { return; }
void AtEOXact_SPI(bool isCommit) { return; }
void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid) { return; }
bool SPI_inside_nonatomic_context(void) { return 0; }
