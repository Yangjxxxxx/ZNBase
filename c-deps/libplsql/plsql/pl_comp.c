/*-------------------------------------------------------------------------
 *
 * pl_comp.c		- Compiler part of the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_comp.c
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql.h"
/* ----------
 * Our own local and global variables
 * ----------
 */
// TODO add by cms, for compile
// 该文件中被注掉的部分主要是 searchCache,tuple,RangeVar,hash相关, 可以搜索TODO到达具体位置
#define DEBUG(format, ...) printf("LINE: %d: "           \
                                  "FUNC %s :" format "", \
                                  __LINE__, __FUNCTION__, ##__VA_ARGS__)

#define PROARGMODE_IN 'i'
#define PROARGMODE_OUT 'o'
#define PROARGMODE_INOUT 'b'
#define PROARGMODE_VARIADIC 'v'
#define PROARGMODE_TABLE 't'

#define PROVOLATILE_VOLATILE 'v' /* can change even within a scan */

#define PROKIND_FUNCTION 'f'
#define PROKIND_PROCEDURE 'p'

#define RELKIND_RELATION 'r'          /* ordinary table */
#define RELKIND_SEQUENCE 'S'          /* sequence object */
#define RELKIND_VIEW 'v'              /* view */
#define RELKIND_MATVIEW 'm'           /* materialized view */
#define RELKIND_COMPOSITE_TYPE 'c'    /* composite type */
#define RELKIND_FOREIGN_TABLE 'f'     /* foreign table */
#define RELKIND_PARTITIONED_TABLE 'p' /* partitioned table */

#define INVALID_TUPLEDESC_IDENTIFIER ((uint64)1)
#define CALLED_AS_TRIGGER(fcinfo) false
#define CALLED_AS_EVENT_TRIGGER(fcinfo) false

bool check_function_bodies = true;
// end of add by cms

PLpgSQL_stmt_block *plpgsql_parse_result;

static int datums_alloc;
int plpgsql_nDatums;
PLpgSQL_datum **plpgsql_Datums;
static int datums_last;

char *plpgsql_error_funcname;
bool plpgsql_DumpExecTree = false;
bool plpgsql_check_syntax = false;

PLpgSQL_function *plpgsql_curr_compile;
bool hashmap_init;
hashmap_map *compile_func_map = NULL;

/* A context appropriate for short-term allocs during compilation */
MemoryContext plpgsql_compile_tmp_cxt;

/* ----------
 * Hash table for compiled functions
 * ----------
 */
//static HTAB *plpgsql_HashTable = NULL;

typedef struct plpgsql_hashent
{
    PLpgSQL_func_hashkey key;
    PLpgSQL_function *function;
} plpgsql_HashEnt;

#define FUNCS_PER_USER 128 /* initial table size */

/* ----------
 * Lookup table for EXCEPTION condition names
 * ----------
 */
typedef struct
{
    const char *label;
    int sqlerrstate;
} ExceptionLabelMap;

// 报错信息的头文件
//#include "plerrcodes.h"			/* pgrminclude ignore */
static const ExceptionLabelMap exception_label_map[] = {
#include "parser/plerrcodes.h"			/* pgrminclude ignore */
    {NULL, 0}
};

/* ----------
 * static prototypes
 * ----------
 */
static PLpgSQL_function *do_compile(FunctionCallInfo fcinfo,
                                    Form_pg_proc procStruct,
                                    PLpgSQL_function *function,
                                    PLpgSQL_func_hashkey *hashkey,
                                    bool forValidator);
static void plpgsql_compile_error_callback(void *arg);
static void add_parameter_name(PLpgSQL_nsitem_type itemtype, int itemno, const char *name);
static void add_dummy_return(PLpgSQL_function *function);
static Node *plpgsql_pre_column_ref(ParseState *pstate, ColumnRef *cref);
static Node *plpgsql_post_column_ref(ParseState *pstate, ColumnRef *cref, Node *var);
static Node *plpgsql_param_ref(ParseState *pstate, ParamRef *pref);
/*static Node *resolve_column_ref(ParseState *pstate, PLpgSQL_expr *expr,
                                ColumnRef *cref, bool error_if_no_field);*/
/*static Node *make_datum_param(PLpgSQL_expr *expr, int dno, int location);*/
static PLpgSQL_row *build_row_from_vars(PLpgSQL_variable **vars, int numvars);
static PLpgSQL_type *build_datatype(Form_pg_type typeDesc, int32 typmod, Oid collation);
static void plpgsql_start_datums(void);
static void plpgsql_finish_datums(PLpgSQL_function *function);
static void compute_function_hashkey(FunctionCallInfo fcinfo,
                                     Form_pg_proc procStruct,
                                     PLpgSQL_func_hashkey *hashkey,
                                     bool forValidator);
static void plpgsql_resolve_polymorphic_argtypes(int numargs,
                                                 Oid *argtypes, char *argmodes,
                                                 Node *call_expr, bool forValidator,
                                                 const char *proname);
static bool split(char *src, const char *separator, char ***dest, int *num);

/* ----------
 * plpgsql_compile		Make an execution tree for a PL/pgSQL function.
 *
 * If forValidator is true, we're only compiling for validation purposes,
 * and so some checks are skipped.
 *
 * Note: it's important for this to fall through quickly if the function
 * has already been compiled.
 * ----------
 */
PLpgSQL_function *plpgsql_compile(Handler_t handler, FunctionCallInfo fcinfo, bool forValidator)
{
    Form_pg_proc procStruct;
    PLpgSQL_function *function = NULL;
    PLpgSQL_func_hashkey hashkey;
    bool function_valid = false;
    bool hashkey_valid = false;
    FormData_pg_proc procReal;
    procReal = SearchFunctionMetaCache(handler, PROC_CATCH_ID);
    procStruct = &procReal;
    if (!function)
    {
        hashkey_valid = true;
    }
    //没有被编译过，我们需要计算函数的hash——key，并且编译函数
    if (!function_valid)
    {
        if (!hashkey_valid)
            compute_function_hashkey(fcinfo, procStruct, &hashkey,
                                     forValidator);
        function = do_compile(fcinfo, procStruct, function,
                              &hashkey, forValidator);
    }
    //將編譯好的function插入map
    //hashmap_put(compile_func_map,fcinfo->hash_of_sql,function);
    //将function的指针保存到fn_extra中
    fcinfo->flinfo->fn_extra = (void *)function;
    return function;
}
//函数名： spilt
//作者：   zyk
//日期     2020-06-03
//功能：    按分隔符分割字符串
//参数：
//  src          元字符串
//  separator    分割符
//  dest         返回的字符串数组（必须分配好空间）
//  num          分割后字符串数组个数
static bool split(char *src, const char *separator, char ***dest, int *num)
{
    char *pNext;
    int count = 0;
    if (src == NULL || strlen(src) == 0)
        return false;
    if (separator == NULL || strlen(separator) == 0)
        return false;
    pNext = strtok(src, separator);
    while (pNext != NULL)
    {
        if (strlen(pNext) > 255) {
            return true;
        }
        strcpy((*dest)[count],pNext);
        count++;
        pNext = strtok(NULL, separator);
    }
    *num = count;
    return false;
}
/*
 *这是plpgsql中最慢的部分
 *function指针不是空指针的话，就是要重新编译并且覆盖的内存
 *在编译一个函数时，CurrentMemoryContext是我们正在编译的函数的每个函数的内存上下文。这意味着palloc()将分配与函数本身具有相同生命周期的存储。
 *
 * Because palloc()'d storage will not be immediately freed, temporary
 * allocations should either be performed in a short-lived memory
 * context or explicitly pfree'd. Since not all backend functions are
 * careful about pfree'ing their allocations, it is also wise to
 * switch into a short-term context before calling into the
 * backend. An appropriate context for performing short-term
 * allocations is the plpgsql_compile_tmp_cxt.
 *
 * 这段代码不可重入，我们假设做的所有操作不能影响到另一个plpgsql function
 */
static PLpgSQL_function *do_compile(FunctionCallInfo fcinfo,
                                    Form_pg_proc procStruct,
                                    PLpgSQL_function *function,
                                    PLpgSQL_func_hashkey *hashkey,
                                    bool forValidator)
{
    Form_pg_type typeStruct;
    PLpgSQL_variable *var;
    int i;
    ErrorContextCallback plerrcontext;
    int parse_rc;
    Oid rettypeid;
    int numargs = 0;
    int num_in_args = 0;
    int num_out_args = 0;
    Oid *argtypes;
    char *argmodes;
    int *in_arg_varnos = NULL;
    PLpgSQL_variable **out_arg_variables;
    MemoryContext func_cxt;
    plpgsql_scanner_init(procStruct->proc_source);

    plpgsql_error_funcname = pstrdup(NameStr(procStruct->proname));
    plerrcontext.callback = plpgsql_compile_error_callback;
    plerrcontext.arg = forValidator ? procStruct->proc_source : NULL;
    plerrcontext.previous = error_context_stack;
    error_context_stack = &plerrcontext;
    /*
     * 在验证函数定义时执行额外的语法检查。出于性能考虑，
     * 在实际编译函数以执行时，我们将跳过这一步骤。所以一般为false
     * (zh): 不能为false，否则不会进行语法检查
     */
    plpgsql_check_syntax = forValidator;
    /*
     * 创建新的函数结构，如果还没有完成。
     * 函数结构永远不会被丢弃，所以要将它们保存在TopMemoryContext中
     */
    if (function == NULL)
    {
        function = (PLpgSQL_function *)
            MemoryContextAllocZero(TopMemoryContext, sizeof(PLpgSQL_function));
    }
    else
    {
        memset(function, 0, sizeof(PLpgSQL_function));
    }
    plpgsql_curr_compile = function;
    /*
     * 编译的所有永久输出(例如解析树)都保存在每个函数的内存上下文中，
     * 因此可以很容易地回收它们。
     */
    func_cxt = AllocSetContextCreate(TopMemoryContext,
                                     "PL/pgSQL function",
                                     ALLOCSET_DEFAULT_SIZES);
    plpgsql_compile_tmp_cxt = MemoryContextSwitchTo(func_cxt);
    function->fn_signature = NULL;
    MemoryContextSetIdentifier(func_cxt, function->fn_signature);
    function->fn_oid = fcinfo->flinfo->fn_oid;
    function->fn_input_collation = fcinfo->fncollation;
    function->fn_cxt = func_cxt;
    function->out_param_varno = -1; /* set up for no OUT param */
    function->resolve_option = plpgsql_variable_conflict;
    function->print_strict_params = plpgsql_print_strict_params;
    function->extra_warnings = forValidator ? plpgsql_extra_warnings : 0;
    function->extra_errors = forValidator ? plpgsql_extra_errors : 0;
    
    // 所有的function都是not trigger状态
    function->fn_is_trigger = PLPGSQL_NOT_TRIGGER;
    function->fn_prokind = procStruct->prokind;

    function->nstatements = 0;
    plpgsql_ns_init();
    plpgsql_ns_push(NameStr(procStruct->proname), PLPGSQL_LABEL_BLOCK);
    plpgsql_DumpExecTree = false;
    plpgsql_start_datums();

    /*
         * Fetch info about the procedure's parameters. Allocations aren't
         * needed permanently, so make them in tmp cxt.
         *
         * We also need to resolve any polymorphic input or output
         * argument types.  In validation mode we won't be able to, so we
         * arbitrarily assume we are dealing with integers.
         */
    MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
    numargs = procStruct->pronargs;
    argtypes = procStruct->argtypes;
    int typetestNum = 0;

    in_arg_varnos = (int *)palloc(numargs * sizeof(int));
    out_arg_variables = (PLpgSQL_variable **)palloc(numargs * sizeof(PLpgSQL_variable *));

    MemoryContextSwitchTo(func_cxt);

    /*
     * Create the variables for the procedure's parameters.
     */
    argmodes = procStruct->argModes;
    for (i = 0; i < numargs; i++)
    {
        char buf[32];
        Oid argtypeid = argtypes[i];
        char argmode = argmodes ? argmodes[i] : PROARGMODE_IN;
        PLpgSQL_type *argdtype;
        PLpgSQL_variable *argvariable;
        PLpgSQL_nsitem_type argitemtype;

        /* Create $n name for variable */
        snprintf(buf, sizeof(buf), "$%d", i + 1);

        /* Create datatype info */
        argdtype = plpgsql_build_datatype(procStruct->argTypeNames[i], argtypeid,
                                          -1,
                                          function->fn_input_collation);
        /* Disallow pseudotype argument */
        /* (note we already replaced polymorphic types) */
        /* (build_variable would do this, but wrong message) */
        if (argdtype->ttype == PLPGSQL_TTYPE_PSEUDO)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("PL/pgSQL functions cannot accept type %s",
                            format_type_be(argtypeid))));

   /*
    * Build variable and add to datum list.  If there's a name
    * for the argument, use that as refname, else use $n name.
    */
        argvariable = plpgsql_build_variable((procStruct->argnames &&
                                              procStruct->argnames[i][0] != '\0')
                                                 ? procStruct->argnames[i]
                                                 : buf,
                                             0, argdtype, false, false);

        if (argvariable->dtype == PLPGSQL_DTYPE_VAR)
        {
            argitemtype = PLPGSQL_NSTYPE_VAR;
        }
        else
        {
            Assert(argvariable->dtype == PLPGSQL_DTYPE_REC);
            argitemtype = PLPGSQL_NSTYPE_REC;
        }

        /* Remember arguments in appropriate arrays */
        if (argmode == PROARGMODE_IN ||
            argmode == PROARGMODE_INOUT ||
            argmode == PROARGMODE_VARIADIC)
            in_arg_varnos[num_in_args++] = argvariable->dno;
        if (argmode == PROARGMODE_OUT ||
            argmode == PROARGMODE_INOUT ||
            argmode == PROARGMODE_TABLE)
            out_arg_variables[num_out_args++] = argvariable;

        /* Add to namespace under the $n name */
        add_parameter_name(argitemtype, argvariable->dno, buf);

        /* If there's a name for the argument, make an alias */
        if (procStruct->argnames && procStruct->argnames[i][0] != '\0')
            add_parameter_name(argitemtype, argvariable->dno,
                               procStruct->argnames[i]);
    }

    /*
         * If there's just one OUT parameter, out_param_varno points
         * directly to it.  If there's more than one, build a row that
         * holds all of them.  Procedures return a row even for one OUT
         * parameter.
         */
    if (num_out_args > 1 ||
        (num_out_args == 1 && function->fn_prokind == PROKIND_PROCEDURE))
    {
        PLpgSQL_row *row = build_row_from_vars(out_arg_variables,
                                               num_out_args);

        plpgsql_adddatum((PLpgSQL_datum *)row);
        function->out_param_varno = row->dno;
    }
    else if (num_out_args == 1)
        function->out_param_varno = out_arg_variables[0]->dno;

    rettypeid = procStruct->prorettype;
    /*
     * Normal function has a defined returntype
     */
    function->fn_rettype = rettypeid;
    function->fn_retset = procStruct->proretset;

    /*
     * Lookup the function's return type
     */

    /* Disallow pseudotype result, except VOID or RECORD */
    /* (note we already replaced polymorphic types) */

    /* Disallow pseudotype result, except VOID or RECORD */
    /* (note we already replaced polymorphic types) */
    Handler_t handler; //
    handler.ErrMsg = NULL;
    handler.magicCode = 0;
    FormData_pg_type stPgType = SearchTypeMetaCache(handler, rettypeid);
    typeStruct = &stPgType;
    if (typeStruct->typtype == TYPTYPE_PSEUDO)
    {
        if (rettypeid == VOIDOID ||
            rettypeid == RECORDOID)
            /* okay */;
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("PL/pgSQL functions cannot return type %s",
                            format_type_be(rettypeid))));
    }

    //TODO: 从源数据中读取
    function->fn_retistuple = false; // type_is_rowtype(rettypeid);
    function->fn_retisdomain = (typeStruct->typtype == TYPTYPE_DOMAIN);
    function->fn_retbyval = typeStruct->typbyval;
    function->fn_rettyplen = typeStruct->typlen;

        /*
         * install $0 reference, but only for polymorphic return types,
         * and not when the return is specified through an output
         * parameter.
         */

    if (IsPolymorphicType(procStruct->prorettype) &&
        num_out_args == 0)
    {
        (void)plpgsql_build_variable("$0", 0,
                                     build_datatype(typeStruct,
                                                    -1,
                                                    function->fn_input_collation),
                                     true, false);
    }

    /* Remember if function is STABLE/IMMUTABLE */
    function->fn_readonly = (procStruct->provolatile != PROVOLATILE_VOLATILE);

    /*
     * Create the magic FOUND variable.
     */
    var = plpgsql_build_variable("found", 0,
                                 plpgsql_build_datatype("bool", BOOLOID,
                                                        -1,
                                                        InvalidOid),
                                 true, false);
    function->found_varno = var->dno;

    /*
     * Now parse the function's text
     */
    parse_rc = plpgsql_yyparse();
    if (parse_rc != 0)
        elog(ERROR, "plpgsql parser returned %d", parse_rc);
    function->action = plpgsql_parse_result;

    plpgsql_scanner_finish();
    pfree(procStruct->proc_source);

    /*
     * If it has OUT parameters or returns VOID or returns a set, we allow
     * control to fall off the end without an explicit RETURN statement. The
     * easiest way to implement this is to add a RETURN statement to the end
     * of the statement list during parsing.
     */
    if (num_out_args > 0 || function->fn_rettype == VOIDOID ||
        function->fn_retset)
        add_dummy_return(function);

    /*
     * Complete the function's info
     */
    function->fn_nargs = num_in_args;
    for (i = 0; i < function->fn_nargs; i++)
        function->fn_argvarnos[i] = in_arg_varnos[i];

    plpgsql_finish_datums(function);

    /* Debug dump for completed functions */
    if (plpgsql_DumpExecTree)
        plpgsql_dumptree(function);

    /*
     * Pop the error context stack
     */
    error_context_stack = plerrcontext.previous;
    plpgsql_error_funcname = NULL;

    plpgsql_check_syntax = false;

    MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
    plpgsql_compile_tmp_cxt = NULL;
    return function;
}

/* ----------
 * plpgsql_compile_inline	Make an execution tree for an anonymous code block.
 *
 * Note: this is generally parallel to do_compile(); is it worth trying to
 * merge the two?
 *
 * Note: we assume the block will be thrown away so there is no need to build
 * persistent data structures.
 * ----------
 */
PLpgSQL_function *
plpgsql_compile_inline(char *proc_source)
{
    char *func_name = "inline_code_block";
    PLpgSQL_function *function;
    ErrorContextCallback plerrcontext;
    PLpgSQL_variable *var;
    int parse_rc;
    MemoryContext func_cxt;

    /*
     * Setup the scanner input and error info.  We assume that this function
     * cannot be invoked recursively, so there's no need to save and restore
     * the static variables used here.
     */
    plpgsql_scanner_init(proc_source);

    plpgsql_error_funcname = func_name;

    /*
     * Setup error traceback support for ereport()
     */
    plerrcontext.callback = plpgsql_compile_error_callback;
    plerrcontext.arg = proc_source;
    plerrcontext.previous = error_context_stack;
    error_context_stack = &plerrcontext;

    /* Do extra syntax checking if check_function_bodies is on */
    plpgsql_check_syntax = check_function_bodies;

    /* Function struct does not live past current statement */
    function = (PLpgSQL_function *)palloc0(sizeof(PLpgSQL_function));

    plpgsql_curr_compile = function;

    /*
     * All the rest of the compile-time storage (e.g. parse tree) is kept in
     * its own memory context, so it can be reclaimed easily.
     */
    func_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                     "PL/pgSQL inline code context",
                                     ALLOCSET_DEFAULT_SIZES);
    plpgsql_compile_tmp_cxt = MemoryContextSwitchTo(func_cxt);

    function->fn_signature = pstrdup(func_name);
    function->fn_is_trigger = PLPGSQL_NOT_TRIGGER;
    function->fn_input_collation = InvalidOid;
    function->fn_cxt = func_cxt;
    function->out_param_varno = -1; /* set up for no OUT param */
    function->resolve_option = plpgsql_variable_conflict;
    function->print_strict_params = plpgsql_print_strict_params;

    /*
     * don't do extra validation for inline code as we don't want to add spam
     * at runtime
     */
    function->extra_warnings = 0;
    function->extra_errors = 0;

    function->nstatements = 0;

    plpgsql_ns_init();
    plpgsql_ns_push(func_name, PLPGSQL_LABEL_BLOCK);
    plpgsql_DumpExecTree = false;
    plpgsql_start_datums();

    /* Set up as though in a function returning VOID */
    function->fn_rettype = VOIDOID;
    function->fn_retset = false;
    function->fn_retistuple = false;
    function->fn_retisdomain = false;
    function->fn_prokind = PROKIND_FUNCTION;
    /* a bit of hardwired knowledge about type VOID here */
    function->fn_retbyval = true;
    function->fn_rettyplen = sizeof(int32);

    /*
     * Remember if function is STABLE/IMMUTABLE.  XXX would it be better to
     * set this true inside a read-only transaction?  Not clear.
     */
    function->fn_readonly = false;

    /*
     * Create the magic FOUND variable.
     */
    var = plpgsql_build_variable("found", 0,
                                 plpgsql_build_datatype("bool", BOOLOID,
                                                        -1,
                                                        InvalidOid),
                                 true, false);
    function->found_varno = var->dno;

    /*
     * Now parse the function's text
     */
    parse_rc = plpgsql_yyparse();
    if (parse_rc != 0)
        elog(ERROR, "plpgsql parser returned %d", parse_rc);
    function->action = plpgsql_parse_result;

    plpgsql_scanner_finish();

    /*
     * If it returns VOID (always true at the moment), we allow control to
     * fall off the end without an explicit RETURN statement.
     */
    if (function->fn_rettype == VOIDOID)
        add_dummy_return(function);

    /*
     * Complete the function's info
     */
    function->fn_nargs = 0;

    plpgsql_finish_datums(function);

    /*
     * Pop the error context stack
     */
    error_context_stack = plerrcontext.previous;
    plpgsql_error_funcname = NULL;

    plpgsql_check_syntax = false;

    MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
    plpgsql_compile_tmp_cxt = NULL;
    return function;
}

/*
 * error context callback to let us supply a call-stack traceback.
 * If we are validating or executing an anonymous code block, the function
 * source text is passed as an argument.
 */
static void
plpgsql_compile_error_callback(void *arg)
{
    if (arg)
    {
        /*
         * Try to convert syntax error position to reference text of original
         * CREATE FUNCTION or DO command.
         */
        // TODO : 重新报错调整位置代码？
        /*if (function_parse_error_transpose((const char *) arg))*/
        /*return;*/

        /*
         * Done if a syntax error position was reported; otherwise we have to
         * fall back to a "near line N" report.
         */
    }

    /*if (plpgsql_error_funcname)*/
    /*errcontext("compilation of PL/pgSQL function \"%s\" near line %d",*/
    /*plpgsql_error_funcname, plpgsql_latest_lineno());*/
}

/*
 * Add a name for a function parameter to the function's namespace
 */
static void
add_parameter_name(PLpgSQL_nsitem_type itemtype, int itemno, const char *name)
{
    /*
     * Before adding the name, check for duplicates.  We need this even though
     * functioncmds.c has a similar check, because that code explicitly
     * doesn't complain about conflicting IN and OUT parameter names.  In
     * plpgsql, such names are in the same namespace, so there is no way to
     * disambiguate.
     */
    if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                          name, NULL, NULL,
                          NULL) != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                 errmsg("parameter name \"%s\" used more than once",
                        name)));

    /* OK, add the name */
    plpgsql_ns_additem(itemtype, itemno, name);
}

/*
 * Add a dummy RETURN statement to the given function's body
 */
static void
add_dummy_return(PLpgSQL_function *function)
{
    /*
     * If the outer block has an EXCEPTION clause, we need to make a new outer
     * block, since the added RETURN shouldn't act like it is inside the
     * EXCEPTION clause.
     */
    if (function->action->exceptions != NULL)
    {
        PLpgSQL_stmt_block *new;

        new = palloc0(sizeof(PLpgSQL_stmt_block));
        new->cmd_type = PLPGSQL_STMT_BLOCK;
        new->stmtid = ++function->nstatements;
        new->body = list_make1(function->action);

        function->action = new;
    }
    if (function->action->body == NIL ||
        ((PLpgSQL_stmt *)llast(function->action->body))->cmd_type != PLPGSQL_STMT_RETURN)
    {
        PLpgSQL_stmt_return *new;

        new = palloc0(sizeof(PLpgSQL_stmt_return));
        new->cmd_type = PLPGSQL_STMT_RETURN;
        new->stmtid = ++function->nstatements;
        new->expr = NULL;
        new->retvarno = function->out_param_varno;

        function->action->body = lappend(function->action->body, new);
    }
}

/*
 * plpgsql_parser_setup		set up parser hooks for dynamic parameters
 *
 * Note: this routine, and the hook functions it prepares for, are logically
 * part of plpgsql parsing.  But they actually run during function execution,
 * when we are ready to evaluate a SQL query or expression that has not
 * previously been parsed and planned.
 */
void plpgsql_parser_setup(struct ParseState *pstate, PLpgSQL_expr *expr)
{
    pstate->p_pre_columnref_hook = plpgsql_pre_column_ref;
    pstate->p_post_columnref_hook = plpgsql_post_column_ref;
    pstate->p_paramref_hook = plpgsql_param_ref;
    /* no need to use p_coerce_param_hook */
    pstate->p_ref_hook_state = (void *)expr;
}

/*
 * plpgsql_pre_column_ref		parser callback before parsing a ColumnRef
 */
static Node *
plpgsql_pre_column_ref(ParseState *pstate, ColumnRef *cref)
{
    PLpgSQL_expr *expr = (PLpgSQL_expr *)pstate->p_ref_hook_state;

    if (expr->func->resolve_option == PLPGSQL_RESOLVE_VARIABLE)
        return resolve_column_ref(pstate, expr, cref, false);
    else
        return NULL;
}

/*
 * plpgsql_post_column_ref		parser callback after parsing a ColumnRef
 */
static Node *
plpgsql_post_column_ref(ParseState *pstate, ColumnRef *cref, Node *var)
{
    PLpgSQL_expr *expr = (PLpgSQL_expr *)pstate->p_ref_hook_state;
    Node *myvar;

    if (expr->func->resolve_option == PLPGSQL_RESOLVE_VARIABLE)
        return NULL; /* we already found there's no match */

    if (expr->func->resolve_option == PLPGSQL_RESOLVE_COLUMN && var != NULL)
        return NULL; /* there's a table column, prefer that */

    /*
     * If we find a record/row variable but can't match a field name, throw
     * error if there was no core resolution for the ColumnRef either.  In
     * that situation, the reference is inevitably going to fail, and
     * complaining about the record/row variable is likely to be more on-point
     * than the core parser's error message.  (It's too bad we don't have
     * access to transformColumnRef's internal crerr state here, as in case of
     * a conflict with a table name this could still be less than the most
     * helpful error message possible.)
     */
    myvar = resolve_column_ref(pstate, expr, cref, (var == NULL));

    if (myvar != NULL && var != NULL)
    {
        /*
         * We could leave it to the core parser to throw this error, but we
         * can add a more useful detail message than the core could.
         */
        // TODO: 修改报错体系
        /*ereport(ERROR,*/
        /*(errcode(ERRCODE_AMBIGUOUS_COLUMN),*/
        /*errmsg("column reference \"%s\" is ambiguous",*/
        /*NameListToString(cref->fields)),*/
        /*errdetail("It could refer to either a PL/pgSQL variable or a table column."),*/
        /*parser_errposition(pstate, cref->location)));*/
    }

    return myvar;
}

/*
 * plpgsql_param_ref		parser callback for ParamRefs ($n symbols)
 */
static Node *
plpgsql_param_ref(ParseState *pstate, ParamRef *pref)
{
    PLpgSQL_expr *expr = (PLpgSQL_expr *)pstate->p_ref_hook_state;
    char pname[32];
    PLpgSQL_nsitem *nse;

    snprintf(pname, sizeof(pname), "$%d", pref->number);

    nse = plpgsql_ns_lookup(expr->ns, false,
                            pname, NULL, NULL,
                            NULL);

    if (nse == NULL)
        return NULL; /* name not known to plpgsql */

    return make_datum_param(expr, nse->itemno, pref->location);
}

/*
 * resolve_column_ref		attempt to resolve a ColumnRef as a plpgsql var
 *
 * Returns the translated node structure, or NULL if name not found
 *
 * error_if_no_field tells whether to throw error or quietly return NULL if
 * we are able to match a record/row name but don't find a field name match.
 */
/*static */Node *
resolve_column_ref(ParseState *pstate, PLpgSQL_expr *expr,
                   ColumnRef *cref, bool error_if_no_field)
{
    PLpgSQL_execstate *estate;
    PLpgSQL_nsitem *nse;
    const char *name1;
    const char *name2 = NULL;
    const char *name3 = NULL;
    const char *colname = NULL;
    int nnames;
    int nnames_scalar = 0;
    int nnames_wholerow = 0;
    int nnames_field = 0;

    /*
     * We use the function's current estate to resolve parameter data types.
     * This is really pretty bogus because there is no provision for updating
     * plans when those types change ...
     */
    estate = expr->func->cur_estate;

    /*----------
     * The allowed syntaxes are:
     *
     * A		Scalar variable reference, or whole-row record reference.
     * A.B		Qualified scalar or whole-row reference, or field reference.
     * A.B.C	Qualified record field reference.
     * A.*		Whole-row record reference.
     * A.B.*	Qualified whole-row record reference.
     *----------
     */
    switch (list_length(cref->fields))
    {
    case 1:
    {
        Node *field1 = (Node *)linitial(cref->fields);

        Assert(IsA(field1, String));
        name1 = strVal(field1);
        nnames_scalar = 1;
        nnames_wholerow = 1;
        break;
    }
    case 2:
    {
        Node *field1 = (Node *)linitial(cref->fields);
        Node *field2 = (Node *)lsecond(cref->fields);

        Assert(IsA(field1, String));
        name1 = strVal(field1);

        /* Whole-row reference? */
        if (IsA(field2, A_Star))
        {
            /* Set name2 to prevent matches to scalar variables */
            name2 = "*";
            nnames_wholerow = 1;
            break;
        }

        Assert(IsA(field2, String));
        name2 = strVal(field2);
        colname = name2;
        nnames_scalar = 2;
        nnames_wholerow = 2;
        nnames_field = 1;
        break;
    }
    case 3:
    {
        Node *field1 = (Node *)linitial(cref->fields);
        Node *field2 = (Node *)lsecond(cref->fields);
        Node *field3 = (Node *)lthird(cref->fields);

        Assert(IsA(field1, String));
        name1 = strVal(field1);
        Assert(IsA(field2, String));
        name2 = strVal(field2);

        /* Whole-row reference? */
        if (IsA(field3, A_Star))
        {
            /* Set name3 to prevent matches to scalar variables */
            name3 = "*";
            nnames_wholerow = 2;
            break;
        }

        Assert(IsA(field3, String));
        name3 = strVal(field3);
        colname = name3;
        nnames_field = 2;
        break;
    }
    default:
        /* too many names, ignore */
        return NULL;
    }

    nse = plpgsql_ns_lookup(expr->ns, false,
                            name1, name2, name3,
                            &nnames);

    if (nse == NULL)
        return NULL; /* name not known to plpgsql */

    switch (nse->itemtype)
    {
    case PLPGSQL_NSTYPE_VAR:
        if (nnames == nnames_scalar)
            return make_datum_param(expr, nse->itemno, cref->location);
        break;
    case PLPGSQL_NSTYPE_REC:
        if (nnames == nnames_wholerow)
            return make_datum_param(expr, nse->itemno, cref->location);
        if (nnames == nnames_field)
        {
            /* colname could be a field in this record */
            PLpgSQL_rec *rec = (PLpgSQL_rec *)estate->datums[nse->itemno];
            int i;

            /* search for a datum referencing this field */
            i = rec->firstfield;
            while (i >= 0)
            {
                PLpgSQL_recfield *fld = (PLpgSQL_recfield *)estate->datums[i];

                Assert(fld->dtype == PLPGSQL_DTYPE_RECFIELD &&
                       fld->recparentno == nse->itemno);
                if (strcmp(fld->fieldname, colname) == 0)
                {
                    return make_datum_param(expr, i, cref->location);
                }
                i = fld->nextfield;
            }

            /*
                 * We should not get here, because a RECFIELD datum should
                 * have been built at parse time for every possible qualified
                 * reference to fields of this record.  But if we do, handle
                 * it like field-not-found: throw error or return NULL.
                 */
            // TODO : 修改报错体系
            /*if (error_if_no_field)*/
            /*ereport(ERROR,*/
            /*(errcode(ERRCODE_UNDEFINED_COLUMN),*/
            /*errmsg("record \"%s\" has no field \"%s\"",*/
            /*(nnames_field == 1) ? name1 : name2,*/
            /*colname),*/
            /*parser_errposition(pstate, cref->location)));*/
        }
        break;
    default:
        elog(ERROR, "unrecognized plpgsql itemtype: %d", nse->itemtype);
    }

    /* Name format doesn't match the plpgsql variable type */
    return NULL;
}

/*
 * Helper for columnref parsing: build a Param referencing a plpgsql datum,
 * and make sure that that datum is listed in the expression's paramnos.
 */
/*static*/ Node *
make_datum_param(PLpgSQL_expr *expr, int dno, int location)
{
    PLpgSQL_execstate *estate;
    PLpgSQL_datum *datum;
    Param *param;
    MemoryContext oldcontext;

    /* see comment in resolve_column_ref */
    estate = expr->func->cur_estate;
    Assert(dno >= 0 && dno < estate->ndatums);
    datum = estate->datums[dno];

    /*
     * Bitmapset must be allocated in function's permanent memory context
     */
    oldcontext = MemoryContextSwitchTo(expr->func->fn_cxt);
    /*expr->paramnos = bms_add_member(expr->paramnos, dno);*/
    MemoryContextSwitchTo(oldcontext);

    param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = dno + 1;
    // TODO 获取类型信息的函数
    plpgsql_exec_get_datum_type_info(estate,
                                      datum,
                                      &param->paramtype,
                                      &param->paramtypmod,
                                      &param->paramcollid);
    param->location = location;

    return (Node *)param;
}

/* ----------
 * plpgsql_parse_word		The scanner calls this to postparse
 *				any single word that is not a reserved keyword.
 *
 * word1 is the downcased/dequoted identifier; it must be palloc'd in the
 * function's long-term memory context.
 *
 * yytxt is the original token text; we need this to check for quoting,
 * so that later checks for unreserved keywords work properly.
 *
 * We attempt to recognize the token as a variable only if lookup is true
 * and the plpgsql_IdentifierLookup context permits it.
 *
 * If recognized as a variable, fill in *wdatum and return true;
 * if not recognized, fill in *word and return false.
 * (Note: those two pointers actually point to members of the same union,
 * but for notational reasons we pass them separately.)
 * ----------
 */
bool plpgsql_parse_word(char *word1, const char *yytxt, bool lookup,
                        PLwdatum *wdatum, PLword *word)
{
    PLpgSQL_nsitem *ns;

    /*
     * We should not lookup variables in DECLARE sections.  In SQL
     * expressions, there's no need to do so either --- lookup will happen
     * when the expression is compiled.
     */
    if (lookup && plpgsql_IdentifierLookup == IDENTIFIER_LOOKUP_NORMAL)
    {
        /*
         * Do a lookup in the current namespace stack
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                               word1, NULL, NULL,
                               NULL);

        if (ns != NULL)
        {
            switch (ns->itemtype)
            {
            case PLPGSQL_NSTYPE_VAR:
            case PLPGSQL_NSTYPE_REC:
                wdatum->datum = plpgsql_Datums[ns->itemno];
                wdatum->ident = word1;
                wdatum->quoted = (yytxt[0] == '"');
                wdatum->idents = NIL;
                return true;

            default:
                /* plpgsql_ns_lookup should never return anything else */
                elog(ERROR, "unrecognized plpgsql itemtype: %d",
                     ns->itemtype);
            }
        }
    }

    /*
     * Nothing found - up to now it's a word without any special meaning for
     * us.
     */
    word->ident = word1;
    word->quoted = (yytxt[0] == '"');
    return false;
}

/* ----------
 * plpgsql_parse_dblword		Same lookup for two words
 *					separated by a dot.
 * ----------
 */
bool plpgsql_parse_dblword(char *word1, char *word2,
                           PLwdatum *wdatum, PLcword *cword)
{
    PLpgSQL_nsitem *ns;
    List *idents;
    int nnames;

    idents = list_make2(makeString(word1),
                        makeString(word2));

    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE)
    {
        /*
         * Do a lookup in the current namespace stack
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                               word1, word2, NULL,
                               &nnames);
        if (ns != NULL)
        {
            switch (ns->itemtype)
            {
            case PLPGSQL_NSTYPE_VAR:
                /* Block-qualified reference to scalar variable. */
                wdatum->datum = plpgsql_Datums[ns->itemno];
                wdatum->ident = NULL;
                wdatum->quoted = false; /* not used */
                wdatum->idents = idents;
                return true;

            case PLPGSQL_NSTYPE_REC:
                if (nnames == 1)
                {
                    /*
                         * First word is a record name, so second word could
                         * be a field in this record.  We build a RECFIELD
                         * datum whether it is or not --- any error will be
                         * detected later.
                         */
                    PLpgSQL_rec *rec;
                    PLpgSQL_recfield *new;

                    rec = (PLpgSQL_rec *)(plpgsql_Datums[ns->itemno]);
                    new = plpgsql_build_recfield(rec, word2);

                    wdatum->datum = (PLpgSQL_datum *)new;
                }
                else
                {
                    /* Block-qualified reference to record variable. */
                    wdatum->datum = plpgsql_Datums[ns->itemno];
                }
                wdatum->ident = NULL;
                wdatum->quoted = false; /* not used */
                wdatum->idents = idents;
                return true;

            default:
                break;
            }
        }
    }

    /* Nothing found */
    cword->idents = idents;
    return false;
}

/* ----------
 * plpgsql_parse_tripword		Same lookup for three words
 *					separated by dots.
 * ----------
 */
bool plpgsql_parse_tripword(char *word1, char *word2, char *word3,
                            PLwdatum *wdatum, PLcword *cword)
{
    PLpgSQL_nsitem *ns;
    List *idents;
    int nnames;

    idents = list_make3(makeString(word1),
                        makeString(word2),
                        makeString(word3));

    /*
     * We should do nothing in DECLARE sections.  In SQL expressions, we
     * really only need to make sure that RECFIELD datums are created when
     * needed.
     */
    if (plpgsql_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE)
    {
        /*
         * Do a lookup in the current namespace stack. Must find a qualified
         * reference, else ignore.
         */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                               word1, word2, word3,
                               &nnames);
        if (ns != NULL && nnames == 2)
        {
            switch (ns->itemtype)
            {
            case PLPGSQL_NSTYPE_REC:
            {
                /*
                     * words 1/2 are a record name, so third word could be
                     * a field in this record.
                     */
                PLpgSQL_rec *rec;
                PLpgSQL_recfield *new;

                rec = (PLpgSQL_rec *)(plpgsql_Datums[ns->itemno]);
                new = plpgsql_build_recfield(rec, word3);

                wdatum->datum = (PLpgSQL_datum *)new;
                wdatum->ident = NULL;
                wdatum->quoted = false; /* not used */
                wdatum->idents = idents;
                return true;
            }

            default:
                break;
            }
        }
    }

    /* Nothing found */
    cword->idents = idents;
    return false;
}

/* ----------
 * plpgsql_parse_wordtype	The scanner found word%TYPE. word can be
 *				a variable name or a basetype.
 *
 * Returns datatype struct, or NULL if no match found for word.
 * ----------
 */
PLpgSQL_type *
plpgsql_parse_wordtype(char *ident)
{
    PLpgSQL_type *dtype;
    PLpgSQL_nsitem *nse;
    /*
     * Do a lookup in the current namespace stack
     */
    nse = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                            ident, NULL, NULL,
                            NULL);
    if (nse != NULL)
    {
        switch (nse->itemtype)
        {
        case PLPGSQL_NSTYPE_VAR:
            return ((PLpgSQL_var *)(plpgsql_Datums[nse->itemno]))->datatype;

            /* XXX perhaps allow REC/ROW here? */

        default:
            return NULL;
        }
    }
    /*
     * Word wasn't found in the namespace stack. Try to find a data type with
     * that name, but ignore shell types and complex types.
     */
    Handler_t handler;
    handler.ErrMsg = NULL;
    Oid oid = SearchTypeOidByName(handler, ident);
    TYPEDESC typedesc = SearchTypeMetaCache(handler, oid);
    int i;
    for (i = 0; i < strlen(ident); i++) {
        typedesc.typname.data[i] = ident[i];
    }
    typedesc.typname.data[i] = '\0';
    Form_pg_type typeStruct = &typedesc;
    if (!typeStruct->typisdefined ||
        typeStruct->typrelid != InvalidOid)
    {
        return NULL;
    }
    dtype = build_datatype(typeStruct, -1,
                           plpgsql_curr_compile->fn_input_collation);

    return dtype;
}

/* ----------
 * plpgsql_parse_cwordtype		Same lookup for compositeword%TYPE
 * ----------
 */
PLpgSQL_type *
plpgsql_parse_cwordtype(List *idents)
{
    // TODO: support in the future
    ereport(ERROR,
            (errmsg("not support composite word type now")));

    PLpgSQL_type *dtype = NULL;
    PLpgSQL_nsitem *nse = NULL;
    Oid classOid = 0;
    Form_pg_class classStruct = NULL;
    MemoryContext oldCxt;

    /* Avoid memory leaks in the long-term function context */
    oldCxt = MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);

    if (list_length(idents) == 2)
    {
        /*
         * Do a lookup in the current namespace stack. We don't need to check
         * number of names matched, because we will only consider scalar
         * variables.
         */
        nse = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                                strVal(linitial(idents)),
                                strVal(lsecond(idents)),
                                NULL,
                                NULL);

        if (nse != NULL && nse->itemtype == PLPGSQL_NSTYPE_VAR)
        {
            dtype = ((PLpgSQL_var *)(plpgsql_Datums[nse->itemno]))->datatype;
            goto done;
        }

        /*
         * First word could also be a table name
         */
        classOid = -1;        
        if (!OidIsValid(classOid))
            goto done;
    }
    else if (list_length(idents) == 3)
    {
        if (!OidIsValid(classOid))
            goto done;
    }
    else
        goto done;

    /*
     * It must be a relation, sequence, view, materialized view, composite
     * type, or foreign table
     */
    if (classStruct->relkind != RELKIND_RELATION &&
        classStruct->relkind != RELKIND_SEQUENCE &&
        classStruct->relkind != RELKIND_VIEW &&
        classStruct->relkind != RELKIND_MATVIEW &&
        classStruct->relkind != RELKIND_COMPOSITE_TYPE &&
        classStruct->relkind != RELKIND_FOREIGN_TABLE &&
        classStruct->relkind != RELKIND_PARTITIONED_TABLE)
        goto done;

    /*
     * Found that - build a compiler type struct in the caller's cxt and
     * return it
     */
    Handler_t handler;
    handler.ErrMsg = NULL;
    FormData_pg_type typeDesc = SearchTypeMetaCache(handler, classOid);
    MemoryContextSwitchTo(oldCxt);
    dtype = build_datatype(&typeDesc,
                           typeDesc.typmod,
                           typeDesc.typcollation); 
    MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
done:
    MemoryContextSwitchTo(oldCxt);
    return dtype;
}

/* ----------
 * plpgsql_parse_wordrowtype		Scanner found word%ROWTYPE.
 *					So word must be a table name.
 * ----------
 */
PLpgSQL_type *
plpgsql_parse_wordrowtype(char *ident)
{
   return plpgsql_build_datatype("", -1, -1, InvalidOid);
}

/* ----------
 * plpgsql_parse_cwordrowtype		Scanner found compositeword%ROWTYPE.
 *			So word must be a namespace qualified table name.
 * ----------
 */
PLpgSQL_type *
plpgsql_parse_cwordrowtype(List *idents)
{
    MemoryContext oldCxt;
    if (list_length(idents) != 2)
        return NULL;
    /* Avoid memory leaks in long-term function context */
    oldCxt = MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
    MemoryContextSwitchTo(oldCxt);
    /* Build and return the row type struct */
    return plpgsql_build_datatype("", /* TODO 考虑传递合适的OID */ -1, -1, InvalidOid);
}

/*
 * plpgsql_build_variable - build a datum-array entry of a given
 * datatype
 *
 * The returned struct may be a PLpgSQL_var or PLpgSQL_rec
 * depending on the given datatype, and is allocated via
 * palloc.  The struct is automatically added to the current datum
 * array, and optionally to the current namespace.
 */
PLpgSQL_variable *
plpgsql_build_variable(const char *refname, int lineno, PLpgSQL_type *dtype,
                       bool add2namespace, bool isCursorArg)
{
    PLpgSQL_variable *result;

    switch (dtype->ttype)
    {
    case PLPGSQL_TTYPE_SCALAR:
    {
        /* Ordinary scalar datatype */
        PLpgSQL_var *var;

        var = palloc0(sizeof(PLpgSQL_var));
        var->dtype = PLPGSQL_DTYPE_VAR;
        var->refname = pstrdup(refname);
        var->lineno = lineno;
        var->datatype = dtype;
        /* other fields are left as 0, might be changed by caller */

        /* preset to NULL */
        var->value = 0;
        var->is_cursor_arg = isCursorArg;
        var->isnull = true;
        var->freeval = false;

        plpgsql_adddatum((PLpgSQL_datum *)var);

        if (add2namespace)
            plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR,
                               var->dno,
                               refname);
        result = (PLpgSQL_variable *)var;
        break;
    }
    case PLPGSQL_TTYPE_REC:
    {
        /* Composite type -- build a record variable */
        PLpgSQL_rec *rec;

        rec = plpgsql_build_record(refname, lineno,
                                   dtype, dtype->typoid,
                                   add2namespace);
        result = (PLpgSQL_variable *)rec;
        break;
    }
    case PLPGSQL_TTYPE_PSEUDO:
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("variable \"%s\" has pseudo-type %s",
                        refname, format_type_be(dtype->typoid))));
        result = NULL; /* keep compiler quiet */
        break;
    default:
        elog(ERROR, "unrecognized ttype: %d", dtype->ttype);
        result = NULL; /* keep compiler quiet */
        break;
    }

    return result;
}

/*
 * Build empty named record variable, and optionally add it to namespace
 */
PLpgSQL_rec *
plpgsql_build_record(const char *refname, int lineno,
                     PLpgSQL_type *dtype, Oid rectypeid,
                     bool add2namespace)
{
    PLpgSQL_rec *rec;

    rec = palloc0(sizeof(PLpgSQL_rec));
    rec->dtype = PLPGSQL_DTYPE_REC;
    rec->refname = pstrdup(refname);
    rec->lineno = lineno;
    /* other fields are left as 0, might be changed by caller */
    rec->datatype = dtype;
    rec->rectypeid = rectypeid;
    rec->firstfield = -1;
    rec->erh = NULL;
    plpgsql_adddatum((PLpgSQL_datum *)rec);
    if (add2namespace)
        plpgsql_ns_additem(PLPGSQL_NSTYPE_REC, rec->dno, rec->refname);

    return rec;
}

/*
 * Build a row-variable data structure given the component variables.
 * Include a rowtupdesc, since we will need to materialize the row result.
 */
static PLpgSQL_row *
build_row_from_vars(PLpgSQL_variable **vars, int numvars)
{
    PLpgSQL_row *row;
    int i;

    row = palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->refname = "(unnamed row)";
    row->lineno = -1;
    row->rowtupdesc = NULL;
    row->nfields = numvars;
    row->fieldnames = palloc(numvars * sizeof(char *));
    row->varnos = palloc(numvars * sizeof(int));

    for (i = 0; i < numvars; i++)
    {
        PLpgSQL_variable *var = vars[i];

        /* Member vars of a row should never be const */
        Assert(!var->isconst);
        switch (var->dtype)
        {
        case PLPGSQL_DTYPE_VAR:
        case PLPGSQL_DTYPE_PROMISE:
            break;
        case PLPGSQL_DTYPE_REC:
            break;
        default:
            elog(ERROR, "unrecognized dtype: %d", var->dtype);
            break;
        }
        row->fieldnames[i] = var->refname;
        row->varnos[i] = var->dno;
    }
    return row;
}

/*
 * Build a RECFIELD datum for the named field of the specified record variable
 *
 * If there's already such a datum, just return it; we don't need duplicates.
 */
PLpgSQL_recfield *
plpgsql_build_recfield(PLpgSQL_rec *rec, const char *fldname)
{
    PLpgSQL_recfield *recfield;
    int i;

    /* search for an existing datum referencing this field */
    i = rec->firstfield;
    while (i >= 0)
    {
        PLpgSQL_recfield *fld = (PLpgSQL_recfield *)plpgsql_Datums[i];

        Assert(fld->dtype == PLPGSQL_DTYPE_RECFIELD &&
               fld->recparentno == rec->dno);
        if (strcmp(fld->fieldname, fldname) == 0)
            return fld;
        i = fld->nextfield;
    }

    /* nope, so make a new one */
    recfield = palloc0(sizeof(PLpgSQL_recfield));
    recfield->dtype = PLPGSQL_DTYPE_RECFIELD;
    recfield->fieldname = pstrdup(fldname);
    recfield->recparentno = rec->dno;
    recfield->rectupledescid = INVALID_TUPLEDESC_IDENTIFIER;

    plpgsql_adddatum((PLpgSQL_datum *)recfield);

    /* now we can link it into the parent's chain */
    recfield->nextfield = rec->firstfield;
    rec->firstfield = recfield->dno;

    return recfield;
}

/*
 * plpgsql_build_datatype
 *		Build PLpgSQL_type struct given type OID, typmod, and collation.
 *
 * If collation is not InvalidOid then it overrides the type's default
 * collation.  But collation is ignored if the datatype is non-collatable.
 */
PLpgSQL_type *
plpgsql_build_datatype(char *typeName, Oid typeOid, int32 typmod, Oid collation)
{
    PLpgSQL_type *typ;
    Handler_t handler;
    handler.ErrMsg = NULL;
    TYPEDESC stPgType = SearchTypeMetaCache(handler, typeOid);
    int i;
    for (i = 0; i < strlen(typeName); i++) {
        stPgType.typname.data[i] = typeName[i];
    }
    stPgType.typname.data[i] = '\0';
    typ = build_datatype(&stPgType, stPgType.typmod, collation);

    return typ;
}

/*
 * Utility subroutine to make a PLpgSQL_type struct given a pg_type entry
 */
static PLpgSQL_type *
build_datatype(Form_pg_type typeStruct, int32 typmod, Oid collation)
{
    PLpgSQL_type *typ;

    // if (!typeStruct->typisdefined)
    //     ereport(ERROR,
    //             (errcode(ERRCODE_UNDEFINED_OBJECT),
    //              errmsg("type \"%s\" is only a shell",
    //                     NameStr(typeStruct->typname))));

    typ = (PLpgSQL_type *)palloc(sizeof(PLpgSQL_type));

    typ->typname = pstrdup(NameStr(typeStruct->typname));
    typ->typoid = typeStruct->oid;
    if (typ->typoid == 1790)
    {
        typeStruct->typtype = TYPTYPE_BASE;
    }

    switch (typeStruct->typtype)
    {
    case TYPTYPE_BASE:
    case TYPTYPE_ENUM:
    case TYPTYPE_RANGE:
        typ->ttype = PLPGSQL_TTYPE_SCALAR;
        break;
    case TYPTYPE_COMPOSITE:
        typ->ttype = PLPGSQL_TTYPE_REC;
        break;
    case TYPTYPE_DOMAIN:
        if (/*TODO : 从源数据获取 type_is_rowtype(typeStruct->typbasetype)*/ 1)
            typ->ttype = PLPGSQL_TTYPE_REC;
        else
            typ->ttype = PLPGSQL_TTYPE_SCALAR;
        break;
    case TYPTYPE_PSEUDO:
        if (typ->typoid == RECORDOID)
            typ->ttype = PLPGSQL_TTYPE_REC;
        else
            typ->ttype = PLPGSQL_TTYPE_PSEUDO;
        break;
    default:
        elog(ERROR, "unrecognized typtype: %d",
             (int)typeStruct->typtype);
        break;
    }
    typ->typlen = typeStruct->typlen;
    typ->typbyval = typeStruct->typbyval;
    typ->typtype = typeStruct->typtype;
    typ->collation = typeStruct->typcollation;
    if (OidIsValid(collation) && OidIsValid(typ->collation))
        typ->collation = collation;
    /* Detect if type is true array, or domain thereof */
    /* NB: this is only used to decide whether to apply expand_array */
    if (typeStruct->typtype == TYPTYPE_BASE)
    {
        /*
         * This test should include what get_element_type() checks.  We also
         * disallow non-toastable array types (i.e. oidvector and int2vector).
         */
        typ->typisarray = (typeStruct->typlen == -1 &&
                           OidIsValid(typeStruct->typelem) &&
                           typeStruct->typstorage != 'p');
    }
    else if (typeStruct->typtype == TYPTYPE_DOMAIN)
    {
        /* we can short-circuit looking up base types if it's not varlena */
        typ->typisarray = (typeStruct->typlen == -1 &&
                           typeStruct->typstorage != 'p' &&
                           OidIsValid(/*TODO : 从源数据获取 get_base_element_type(typeStruct->typbasetype)*/ 1));
    }
    else
        typ->typisarray = false;
    typ->atttypmod = typmod;

    return typ;
}

/*
 *	plpgsql_recognize_err_condition
 *		Check condition name and translate it to SQLSTATE.
 *
 * Note: there are some cases where the same condition name has multiple
 * entries in the table.  We arbitrarily return the first match.
 */
int plpgsql_recognize_err_condition(const char *condname, bool allow_sqlstate)
{
    int i;

    if (allow_sqlstate)
    {
        if (strlen(condname) == 5 &&
            strspn(condname, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") == 5)
            return MAKE_SQLSTATE(condname[0],
                                 condname[1],
                                 condname[2],
                                 condname[3],
                                 condname[4]);
    }

    for (i = 0; exception_label_map[i].label != NULL; i++)
    {
        if (strcmp(condname, exception_label_map[i].label) == 0)
            return exception_label_map[i].sqlerrstate;
    }

    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
             errmsg("unrecognized exception condition \"%s\"",
                    condname)));
    return 0; /* keep compiler quiet */
}

/*
 * plpgsql_parse_err_condition
 *		Generate PLpgSQL_condition entry(s) for an exception condition name
 *
 * This has to be able to return a list because there are some duplicate
 * names in the table of error code names.
 */
PLpgSQL_condition *
plpgsql_parse_err_condition(char *condname)
{
    int i;
    PLpgSQL_condition *new;
    PLpgSQL_condition *prev;

    /*
     * XXX Eventually we will want to look for user-defined exception names
     * here.
     */

    /*
     * OTHERS is represented as code 0 (which would map to '00000', but we
     * have no need to represent that as an exception condition).
     */
    if (strcmp(condname, "others") == 0)
    {
        new = palloc(sizeof(PLpgSQL_condition));
        new->sqlerrstate = 0;
        new->condname = condname;
        new->next = NULL;
        return new;
    }
    prev = NULL;
    for (i = 0; exception_label_map[i].label != NULL; i++)
    {
        if (strcmp(condname, exception_label_map[i].label) == 0)
        {
            new = palloc(sizeof(PLpgSQL_condition));
            new->sqlerrstate = exception_label_map[i].sqlerrstate;
            new->condname = condname;
            new->next = prev;
            prev = new;
        }
    }

    if (!prev)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("unrecognized exception condition \"%s\"",
                        condname)));

    return prev;
}

/* ----------
 * plpgsql_start_datums			Initialize datum list at compile startup.
 * ----------
 */
static void
plpgsql_start_datums(void)
{
    datums_alloc = 128;
    plpgsql_nDatums = 0;
    /* This is short-lived, so needn't allocate in function's cxt */
    plpgsql_Datums = MemoryContextAlloc(plpgsql_compile_tmp_cxt,
                                        sizeof(PLpgSQL_datum *) * datums_alloc);
    /* datums_last tracks what's been seen by plpgsql_add_initdatums() */
    datums_last = 0;
}

/* ----------
 * plpgsql_adddatum			Add a variable, record or row
 *					to the compiler's datum list.
 * ----------
 */
void plpgsql_adddatum(PLpgSQL_datum *newdatum)
{
    if (plpgsql_nDatums == datums_alloc)
    {
        datums_alloc *= 2;
        plpgsql_Datums = repalloc(plpgsql_Datums, sizeof(PLpgSQL_datum *) * datums_alloc);
    }

    newdatum->dno = plpgsql_nDatums;
    plpgsql_Datums[plpgsql_nDatums++] = newdatum;
}

/* ----------
 * plpgsql_finish_datums	Copy completed datum info into function struct.
 * ----------
 */
static void
plpgsql_finish_datums(PLpgSQL_function *function)
{
    Size copiable_size = 0;
    int i;

    function->ndatums = plpgsql_nDatums;
    function->datums = palloc(sizeof(PLpgSQL_datum *) * plpgsql_nDatums);
    for (i = 0; i < plpgsql_nDatums; i++)
    {
        function->datums[i] = plpgsql_Datums[i];

        /* This must agree with copy_plpgsql_datums on what is copiable */
        switch (function->datums[i]->dtype)
        {
        case PLPGSQL_DTYPE_VAR:
        case PLPGSQL_DTYPE_PROMISE:
            copiable_size += MAXALIGN(sizeof(PLpgSQL_var));
            break;
        case PLPGSQL_DTYPE_REC:
            copiable_size += MAXALIGN(sizeof(PLpgSQL_rec));
            break;
        default:
            break;
        }
    }
    function->copiable_size = copiable_size;
}

/* ----------
 * plpgsql_add_initdatums		Make an array of the datum numbers of
 *					all the initializable datums created since the last call
 *					to this function.
 *
 * If varnos is NULL, we just forget any datum entries created since the
 * last call.
 *
 * This is used around a DECLARE section to create a list of the datums
 * that have to be initialized at block entry.  Note that datums can also
 * be created elsewhere than DECLARE, eg by a FOR-loop, but it is then
 * the responsibility of special-purpose code to initialize them.
 * ----------
 */
int plpgsql_add_initdatums(int **varnos)
{
    int i;
    int n = 0;

    /*
     * The set of dtypes recognized here must match what exec_stmt_block()
     * cares about (re)initializing at block entry.
     */
    for (i = datums_last; i < plpgsql_nDatums; i++)
    {
        switch (plpgsql_Datums[i]->dtype)
        {
        case PLPGSQL_DTYPE_VAR:
        case PLPGSQL_DTYPE_REC:
            n++;
            break;

        default:
            break;
        }
    }

    if (varnos != NULL)
    {
        if (n > 0)
        {
            *varnos = (int *)palloc(sizeof(int) * n);

            n = 0;
            for (i = datums_last; i < plpgsql_nDatums; i++)
            {
                switch (plpgsql_Datums[i]->dtype)
                {
                case PLPGSQL_DTYPE_VAR:
                case PLPGSQL_DTYPE_REC:
                    (*varnos)[n++] = plpgsql_Datums[i]->dno;

                default:
                    break;
                }
            }
        }
        else
            *varnos = NULL;
    }

    datums_last = plpgsql_nDatums;
    return n;
}

/*
 * Compute the hashkey for a given function invocation
 *
 * The hashkey is returned into the caller-provided storage at *hashkey.
 */
static void
compute_function_hashkey(FunctionCallInfo fcinfo,
                         Form_pg_proc procStruct,
                         PLpgSQL_func_hashkey *hashkey,
                         bool forValidator)
{
    /* Make sure any unused bytes of the struct are zero */
    MemSet(hashkey, 0, sizeof(PLpgSQL_func_hashkey));

    /* get function OID */
    hashkey->funcOid = fcinfo->flinfo->fn_oid;

    /* get input collation, if known */
    hashkey->inputCollation = fcinfo->fncollation;

    if (procStruct->pronargs > 0)
    {
        /* get the argument types */
        memcpy(hashkey->argtypes, procStruct->proargtypes.values,
               procStruct->pronargs * sizeof(Oid));

        /* resolve any polymorphic argument types */
        plpgsql_resolve_polymorphic_argtypes(procStruct->pronargs,
                                             hashkey->argtypes,
                                             NULL,
                                             fcinfo->sflinfo.fn_expr,
                                             forValidator,
                                             NameStr(procStruct->proname));
    }
}

/*
 * This is the same as the standard resolve_polymorphic_argtypes() function,
 * but with a special case for validation: assume that polymorphic arguments
 * are integer, integer-array or integer-range.  Also, we go ahead and report
 * the error if we can't resolve the types.
 */
static void
plpgsql_resolve_polymorphic_argtypes(int numargs,
                                     Oid *argtypes, char *argmodes,
                                     Node *call_expr, bool forValidator,
                                     const char *proname)
{
    int i;

    if (!forValidator)
    {
        /* normal case, pass to standard routine */
        // TODO : 重写多态检查
        /*
        if (!resolve_polymorphic_argtypes(numargs, argtypes, argmodes,
                                          call_expr))
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("could not determine actual argument "
                                   "type for polymorphic function \"%s\"",
                                   proname)));
                                   */
    }
    else
    {
        /* special validation case */
        for (i = 0; i < numargs; i++)
        {
            switch (argtypes[i])
            {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID: /* XXX dubious */
                argtypes[i] = INT4OID;
                break;
            case ANYARRAYOID:
                argtypes[i] = INT4ARRAYOID;
                break;
            case ANYRANGEOID:
                argtypes[i] = INT4RANGEOID;
                break;
            default:
                break;
            }
        }
    }
}
