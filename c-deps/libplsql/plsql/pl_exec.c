/*-------------------------------------------------------------------------
 *
 * pl_exec.c		- Executor for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql.h"
#include "string.h"
#include <ctype.h> 
//To reduce the risk of interference between different SPI callers
uint64 SPI_processed = 0;
SPITupleTable *SPI_tuptable = NULL;

typedef struct
{
    int nargs;     /* number of arguments */
    Oid *types;    /* types of arguments */
    Datum *values; /* evaluated argument values */
    char *nulls;   /* null markers (' '/'n' style) */
} PreparedParamsData;

/*
 * All plpgsql function executions within a single transaction share the same
 * executor EState for evaluating "simple" expressions.  Each function call
 * creates its own "eval_econtext" ExprContext within this estate for
 * per-evaluation workspace.  eval_econtext is freed at normal function exit,
 * and the EState is freed at transaction end (in case of error, we assume
 * that the abort mechanisms clean it all up).  Furthermore, any exception
 * block within a function has to have its own eval_econtext separate from
 * the containing function's, so that we can clean up ExprContext callbacks
 * properly at subtransaction exit.  We maintain a stack that tracks the
 * individual econtexts so that we can clean up correctly at subxact exit.
 *
 * This arrangement is a bit tedious to maintain, but it's worth the trouble
 * so that we don't have to re-prepare simple expressions on each trip through
 * a function.  (We assume the case to optimize is many repetitions of a
 * function within a transaction.)
 *
 * However, there's no value in trying to amortize simple expression setup
 * across multiple executions of a DO block (inline code block), since there
 * can never be any.  If we use the shared EState for a DO block, the expr
 * state trees are effectively leaked till end of transaction, and that can
 * add up if the user keeps on submitting DO blocks.  Therefore, each DO block
 * has its own simple-expression EState, which is cleaned up at exit from
 * plpgsql_inline_handler().  DO blocks still use the simple_econtext_stack,
 * though, so that subxact abort cleanup does the right thing.
 */
typedef struct SimpleEcontextStackEntry
{
    ExprContext *stack_econtext;           /* a stacked econtext */
    SubTransactionId xact_subxid;          /* ID for current subxact */
    struct SimpleEcontextStackEntry *next; /* next stack entry up */
} SimpleEcontextStackEntry;

static EState *shared_simple_eval_estate = NULL;
static SimpleEcontextStackEntry *simple_econtext_stack = NULL;
SPITupleTable*
getTupleUseDBEngine(Handler_t handler);

/*
 * Memory management within a plpgsql function generally works with three
 * contexts:
 *
 * 1. Function-call-lifespan data, such as variable values, is kept in the
 * "main" context, a/k/a the "SPI Proc" context established by SPI_connect().
 * This is usually the CurrentMemoryContext while running code in this module
 * (which is not good, because careless coding can easily cause
 * function-lifespan memory leaks, but we live with it for now).
 *
 * 2. Some statement-execution routines need statement-lifespan workspace.
 * A suitable context is created on-demand by get_stmt_mcontext(), and must
 * be reset at the end of the requesting routine.  Error recovery will clean
 * it up automatically.  Nested statements requiring statement-lifespan
 * workspace will result in a stack of such contexts, see push_stmt_mcontext().
 *
 * 3. We use the eval_econtext's per-tuple memory context for expression
 * evaluation, and as a general-purpose workspace for short-lived allocations.
 * Such allocations usually aren't explicitly freed, but are left to be
 * cleaned up by a context reset, typically done by exec_eval_cleanup().
 *
 * These macros are for use in making short-lived allocations:
 */
#define get_eval_mcontext(estate) \
    ((estate)->eval_econtext->ecxt_per_tuple_memory)
#define eval_mcontext_alloc(estate, sz) \
    MemoryContextAlloc(get_eval_mcontext(estate), sz)
#define eval_mcontext_alloc0(estate, sz) \
    MemoryContextAllocZero(get_eval_mcontext(estate), sz)

/*
 * We use a session-wide hash table for caching cast information.
 *
 * Once built, the compiled expression trees (cast_expr fields) survive for
 * the life of the session.  At some point it might be worth invalidating
 * those after pg_cast changes, but for the moment we don't bother.
 *
 * The evaluation state trees (cast_exprstate) are managed in the same way as
 * simple expressions (i.e., we assume cast expressions are always simple).
 *
 * As with simple expressions, DO blocks don't use the shared hash table but
 * must have their own.  This isn't ideal, but we don't want to deal with
 * multiple simple_eval_estates within a DO block.
 */
typedef struct /* lookup key for cast info */
{
    /* NB: we assume this struct contains no padding bytes */
    Oid srctype;     /* source type for cast */
    Oid dsttype;     /* destination type for cast */
    int32 srctypmod; /* source typmod for cast */
    int32 dsttypmod; /* destination typmod for cast */
} plpgsql_CastHashKey;

typedef struct /* cast_hash table entry */
{
    plpgsql_CastHashKey key;      /* hash key --- MUST BE FIRST */
    Expr *cast_expr;              /* cast expression, or NULL if no-op cast */
    CachedExpression *cast_cexpr; /* cached expression backing the above */
    /* ExprState is valid only when cast_lxid matches current LXID */
    ExprState *cast_exprstate; /* expression's eval tree */
    bool cast_in_use;          /* true while we're executing eval tree */
    LocalTransactionId cast_lxid;
} plpgsql_CastHashEntry;


/*
 * LOOP_RC_PROCESSING encapsulates common logic for looping statements to
 * handle return/exit/continue result codes from the loop body statement(s).
 * It's meant to be used like this:
 *
 *		int rc = PLPGSQL_RC_OK;
 *		for (...)
 *		{
 *			...
 *			rc = exec_stmts(estate, stmt->body);
 *			LOOP_RC_PROCESSING(stmt->label, break);
 *			...
 *		}
 *		return rc;
 *
 * If execution of the loop should terminate, LOOP_RC_PROCESSING will execute
 * "exit_action" (typically a "break" or "goto"), after updating "rc" to the
 * value the current statement should return.  If execution should continue,
 * LOOP_RC_PROCESSING will do nothing except reset "rc" to PLPGSQL_RC_OK.
 *
 * estate and rc are implicit arguments to the macro.
 * estate->exitlabel is examined and possibly updated.
 */
#define LOOP_RC_PROCESSING(looplabel, exit_action)                           \
    if (rc == PLPGSQL_RC_RETURN)                                             \
    {                                                                        \
        /* RETURN, so propagate RC_RETURN out */                             \
        exit_action;                                                         \
    }                                                                        \
    else if (rc == PLPGSQL_RC_EXIT)                                          \
    {                                                                        \
        if (estate->exitlabel == NULL)                                       \
        {                                                                    \
            /* unlabelled EXIT terminates this loop */                       \
            rc = PLPGSQL_RC_OK;                                              \
            exit_action;                                                     \
        }                                                                    \
        else if ((looplabel) != NULL &&                                      \
                 strcmp(looplabel, estate->exitlabel) == 0)                  \
        {                                                                    \
            /* labelled EXIT matching this loop, so terminate loop */        \
            estate->exitlabel = NULL;                                        \
            rc = PLPGSQL_RC_OK;                                              \
            exit_action;                                                     \
        }                                                                    \
        else                                                                 \
        {                                                                    \
            /* non-matching labelled EXIT, propagate RC_EXIT out */          \
            exit_action;                                                     \
        }                                                                    \
    }                                                                        \
    else if (rc == PLPGSQL_RC_CONTINUE)                                      \
    {                                                                        \
        if (estate->exitlabel == NULL)                                       \
        {                                                                    \
            /* unlabelled CONTINUE matches this loop, so continue in loop */ \
            rc = PLPGSQL_RC_OK;                                              \
        }                                                                    \
        else if ((looplabel) != NULL &&                                      \
                 strcmp(looplabel, estate->exitlabel) == 0)                  \
        {                                                                    \
            /* labelled CONTINUE matching this loop, so continue in loop */  \
            estate->exitlabel = NULL;                                        \
            rc = PLPGSQL_RC_OK;                                              \
        }                                                                    \
        else                                                                 \
        {                                                                    \
            /* non-matching labelled CONTINUE, propagate RC_CONTINUE out */  \
            exit_action;                                                     \
        }                                                                    \
    }                                                                        \
    else                                                                     \
        Assert(rc == PLPGSQL_RC_OK)

/************************************************************
 * Local function forward declarations
 ************************************************************/
static void add_unvisited_variable_to_go(PLpgSQL_execstate *estate, 
                                        PLpgSQL_expr *expr);
static void copy_plpgsql_datums(PLpgSQL_execstate *estate,
                                PLpgSQL_function *func);
static MemoryContext get_stmt_mcontext(PLpgSQL_execstate *estate);
static void push_stmt_mcontext(PLpgSQL_execstate *estate);
static void pop_stmt_mcontext(PLpgSQL_execstate *estate);

static int exec_stmt_block(PLpgSQL_execstate *estate,
                           PLpgSQL_stmt_block *block);
static int exec_stmts(PLpgSQL_execstate *estate,
                      List *stmts);
static int exec_stmt(PLpgSQL_execstate *estate,
                     PLpgSQL_stmt *stmt);
static int exec_stmt_assign(PLpgSQL_execstate *estate,
                            PLpgSQL_stmt_assign *stmt);
static int exec_stmt_perform(PLpgSQL_execstate *estate,
                             PLpgSQL_stmt_perform *stmt);
static int exec_stmt_call(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_call *stmt);
static int exec_stmt_getdiag(PLpgSQL_execstate *estate,
                             PLpgSQL_stmt_getdiag *stmt);
static int exec_stmt_if(PLpgSQL_execstate *estate,
                        PLpgSQL_stmt_if *stmt);
static int exec_stmt_case(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_case *stmt);
static int exec_stmt_loop(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_loop *stmt);
static int exec_stmt_while(PLpgSQL_execstate *estate,
                           PLpgSQL_stmt_while *stmt);
static int exec_stmt_fori(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_fori *stmt);
static int exec_stmt_fors(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_fors *stmt);
static int exec_stmt_forc(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_forc *stmt);
static int exec_stmt_foreach_a(PLpgSQL_execstate *estate,
                               PLpgSQL_stmt_foreach_a *stmt);
static int exec_stmt_open(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_open *stmt);
static int exec_stmt_fetch(PLpgSQL_execstate *estate,
                           PLpgSQL_stmt_fetch *stmt);
static int exec_stmt_close(PLpgSQL_execstate *estate,
                           PLpgSQL_stmt_close *stmt);
static int exec_stmt_exit(PLpgSQL_execstate *estate,
                          PLpgSQL_stmt_exit *stmt);
static int exec_stmt_return(PLpgSQL_execstate *estate,
                            PLpgSQL_stmt_return *stmt);
static int exec_stmt_return_next(PLpgSQL_execstate *estate,
                                 PLpgSQL_stmt_return_next *stmt);
static int exec_stmt_return_query(PLpgSQL_execstate *estate,
                                  PLpgSQL_stmt_return_query *stmt);
static int exec_stmt_raise(PLpgSQL_execstate *estate,
                           PLpgSQL_stmt_raise *stmt);
static int exec_stmt_assert(PLpgSQL_execstate *estate,
                            PLpgSQL_stmt_assert *stmt);
static int exec_stmt_execsql(PLpgSQL_execstate *estate,      PLpgSQL_stmt_execsql *stmt);
static int exec_stmt_dynexecute(PLpgSQL_execstate *estate,
                                PLpgSQL_stmt_dynexecute *stmt);
static int exec_stmt_dynfors(PLpgSQL_execstate *estate,
                             PLpgSQL_stmt_dynfors *stmt);
static int exec_stmt_commit(PLpgSQL_execstate *estate,
                            PLpgSQL_stmt_commit *stmt);
static int exec_stmt_set(PLpgSQL_execstate *estate,
                         PLpgSQL_stmt_set *stmt);

static void plpgsql_estate_setup(PLpgSQL_execstate *estate,
                                 PLpgSQL_function *func,
                                 ReturnSetInfo *rsi,
                                 EState *simple_eval_estate);
static void exec_eval_cleanup(PLpgSQL_execstate *estate);

static void exec_prepare_plan(PLpgSQL_execstate *estate,
                              PLpgSQL_expr *expr, int cursorOptions,
                              bool keepplan);
static void exec_check_rw_parameter(PLpgSQL_expr *expr, int target_dno);
static bool contains_target_param(Node *node, int *target_dno);

static void exec_assign_expr(PLpgSQL_execstate *estate,
                             PLpgSQL_datum* target,
                             PLpgSQL_expr *expr,
                             int targetno);
static void exec_assign_c_string(PLpgSQL_execstate *estate,
                                 PLpgSQL_datum *target,
                                 const char *str);
static void exec_assign_value(PLpgSQL_execstate *estate, PLpgSQL_datum *target, Datum value, bool isNull, Oid valtype,
                              int32 valtypmod, bool inFetch,int,int, bool inForEach, int index);
static void exec_eval_datum(PLpgSQL_execstate *estate,
                            PLpgSQL_datum *datum,
                            Oid *typeid,
                            int32 *typetypmod,
                            Datum *value,
                            bool *isnull);
static int exec_eval_integer(PLpgSQL_execstate *estate,
                             PLpgSQL_expr *expr,
                             bool *isNull);
static bool exec_eval_boolean(PLpgSQL_execstate *estate,
                              PLpgSQL_expr *expr,
                              bool *isNull);
static Datum
exec_eval_expr(PLpgSQL_execstate *estate, PLpgSQL_expr *expr, PLpgSQL_datum *target, bool *isNull, Oid *rettype,
               int32 *rettypmod);
static int exec_run_select(PLpgSQL_execstate *estate,
                           PLpgSQL_expr *expr, long maxtuples, Portal *portalP);
static int exec_for_query(PLpgSQL_execstate *estate, PLpgSQL_stmt_forq *stmt,
                          Portal portal, bool prefetch_ok);
static ParamListInfo setup_param_list(PLpgSQL_execstate *estate,
                                      PLpgSQL_expr *expr);
static void
exec_move_row(PLpgSQL_execstate *estate, PLpgSQL_variable *target, HeapTuple tup, TupleDesc tupdesc, bool inFetch);
static ExpandedRecordHeader *make_expanded_record_for_rec(PLpgSQL_execstate *estate,
                                                          PLpgSQL_rec *rec,
                                                          TupleDesc srctupdesc,
                                                          ExpandedRecordHeader *srcerh);
static void exec_move_row_from_fields(PLpgSQL_execstate *estate, PLpgSQL_variable *target, ExpandedRecordHeader *newerh,
                                      Datum *values, bool *nulls, TupleDesc tupdesc, bool inFetch);
static bool compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc);
static HeapTuple make_tuple_from_row(PLpgSQL_execstate *estate,
                                     PLpgSQL_row *row,
                                     TupleDesc tupdesc);
static void exec_move_row_from_datum(PLpgSQL_execstate *estate,
                                     PLpgSQL_variable *target,
                                     Datum value, bool inFetch);
static void instantiate_empty_record_variable(PLpgSQL_execstate *estate,
                                              PLpgSQL_rec *rec);
static char *convert_value_to_string(PLpgSQL_execstate *estate,
                                     Datum value, Oid valtype);
static Datum convert_string_to_value(PLpgSQL_execstate *estate,
                                     char* str, Oid valtype);
static Datum exec_cast_value(PLpgSQL_execstate *estate,
                             Datum value, bool *isnull,
                             Oid valtype, int32 valtypmod,
                             Oid reqtype, int32 reqtypmod);
static void exec_set_found(PLpgSQL_execstate *estate, bool state);
static void plpgsql_create_econtext(PLpgSQL_execstate *estate);
static void plpgsql_destroy_econtext(PLpgSQL_execstate *estate);
static void assign_simple_var(PLpgSQL_execstate *estate, PLpgSQL_var *var,
                              bool isnull, bool freeable);
static void assign_text_var(PLpgSQL_execstate *estate, PLpgSQL_var *var,
                            const char *str);
static void assign_record_var(PLpgSQL_execstate *estate, PLpgSQL_rec *rec,
                              ExpandedRecordHeader *erh, bool inFetch);
static PreparedParamsData *exec_eval_using_params(PLpgSQL_execstate *estate,
                                                  List *params);
static Portal exec_dynquery_with_params(PLpgSQL_execstate *estate,
                                        PLpgSQL_expr *dynquery, List *params,
                                        const char *portalname, int cursorOptions);
static char *format_expr_params(PLpgSQL_execstate *estate,
                                const PLpgSQL_expr *expr);
static char *format_preparedparamsdata(PLpgSQL_execstate *estate,
                                       const PreparedParamsData *ppd);
static void copy_ZNBASE_datums(PLpgSQL_execstate *estate,Handler_t handler);

TupleDesc makeEmptyTupleDesc(CursorFetchPre cfp);

List *
expand_function_arguments(List *args, Oid result_type, Form_pg_proc funcform);

/* ----------
 * plpgsql_exec_function	Called by the call handler for
 *				function execution.
 *
 * This is also used to execute inline code blocks (DO blocks).  The only
 * difference that this code is aware of is that for a DO block, we want
 * to use a private simple_eval_estate, which is created and passed in by
 * the caller.  For regular functions, pass NULL, which implies using
 * shared_simple_eval_estate.  (When using a private simple_eval_estate,
 * we must also use a private cast hashtable, but that's taken care of
 * within plpgsql_estate_setup.)
 * ----------
 */
Datum plpgsql_exec_function(Handler_t handler, PLpgSQL_function *func, FunctionCallInfo fcinfo,
                            EState *simple_eval_estate, bool atomic)
{
    PLpgSQL_execstate estate;
    ErrorContextCallback plerrcontext;
    int i;
    int rc;

    /*
	 * Setup the execution state
	 */
    plpgsql_estate_setup(&estate, func, (ReturnSetInfo *)fcinfo->resultinfo,
                         simple_eval_estate);
    estate.atomic = atomic;
    estate.handler = handler;
    plerrcontext.arg = &estate;
    plerrcontext.previous = error_context_stack;
    error_context_stack = &plerrcontext;

    /*
	 * Make local execution copies of all the datums
	 */
    estate.err_text = gettext_noop("during initialization of execution state");
    copy_plpgsql_datums(&estate, func);
    copy_ZNBASE_datums(&estate,handler);
    /*
	 * Store the actual call argument values into the appropriate variables
	 */
    estate.err_text = gettext_noop("while storing call arguments into local variables");
    /*
     *set func's nargs
     */
    for (i = 0; i < func->fn_nargs; i++)
    {
        int n = func->fn_argvarnos[i];

        switch (estate.datums[n]->dtype)
        {
        case PLPGSQL_DTYPE_VAR:
        {
            PLpgSQL_var *var = (PLpgSQL_var *)estate.datums[n];
            assign_simple_var(&estate, var,
                              true,
                              false);
        }
        break;

        // case PLPGSQL_DTYPE_REC:
        // {
        //     PLpgSQL_rec *rec = (PLpgSQL_rec *)estate.datums[n];

        //     if (!fcinfo->args[i].isnull)
        //     {
        //         /* Assign row value from composite datum */
        //         exec_move_row_from_datum(&estate,
        //                                  (PLpgSQL_variable *)rec,
        //                                  fcinfo->args[i].value, false);
        //     }
        //     else
        //     {
        //         /* If arg is null, set variable to null */
        //         exec_move_row(&estate, (PLpgSQL_variable *) rec,
        //                       NULL, NULL, 0);
        //     }
        //     /* clean up after exec_move_row() */
        //     exec_eval_cleanup(&estate);
        // }
        break;

        default:
            /* Anything else should not be an argument variable */
            elog(ERROR, "unrecognized dtype: %d", func->datums[i]->dtype);
        }
    }
    
    estate.err_text = gettext_noop("during function entry");
    /*
	 * Now call the toplevel block of statements
	 */
    estate.err_text = NULL;
    estate.err_stmt = (PLpgSQL_stmt *)(func->action);
    rc = exec_stmt(&estate, (PLpgSQL_stmt *)func->action);
    char *err = BepiTryToCloseAllUnClosedCursor(estate.handler);
    if (strlen(err) != 0) {
        ereport(ERROR,
                    (errmsg("error happen when close all unclosed cursors: %s", err)));
    }
    if (func->fn_prokind != PROKIND_PROCEDURE) {
        if (rc != PLPGSQL_RC_RETURN)
        {
            estate.err_stmt = NULL;
            estate.err_text = NULL;
            ereport(ERROR,
                            (errcode(ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT),
                             errmsg("control reached end of function without RETURN")));
            plpgsql_destroy_econtext(&estate);
            exec_eval_cleanup(&estate);
            return (Datum)0;
        }
    }

    /*
	 * We got a return value - process it
	 */
    estate.err_stmt = NULL;
    estate.err_text = gettext_noop("while casting return value to function's return type");

    fcinfo->isnull = estate.retisnull;

    /* Clean up any leftover temporary memory */
    plpgsql_destroy_econtext(&estate);
    exec_eval_cleanup(&estate);

    /*
	 * Pop the error context stack
	 */
    error_context_stack = plerrcontext.previous;

    /*
	 * Return the function's result
	 */
    return estate.retval;
}



static void
copy_ZNBASE_datums(PLpgSQL_execstate *estate,Handler_t handler){
    bool ret = false;
    for (int i = 0 ; i < estate->ndatums;i++){
        PLpgSQL_var* var = (PLpgSQL_var*)(estate->datums[i]);
        if (var->datatype == NULL) {
            PLpgSQL_type datyp ;
            var->datatype = &datyp;
            var->datatype->typoid = 0;
        }
        char *default_val_query = "";
        if (var->default_val != NULL) {
            default_val_query = var->default_val->query;
        }
        ret = DeclareDrDBVarible(handler,var->refname,var->datatype->typname,i, var->is_cursor_arg, var->notnull, default_val_query, &handler.ErrMsg);
        if (!ret){
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg(handler.ErrMsg)));
        }
    }
}

/*
 * Author: zhanghao
 * FuncName: add_unvisited_variable_to_go
 * Usage:  add unadded variable to go side to udrRunParams.params to enable resolve variables
 * Args:   
 *      estate
 *      expr
 * Descriptor:
 *      visit expr.ns and add the unused variables to UDRrunParams.params
 *
 *      in the future, can expand this function to implement variable namespace isolate
 * Date: 2020/10/19
 */
static void add_unvisited_variable_to_go(PLpgSQL_execstate *estate, PLpgSQL_expr *expr) {
    if (expr == NULL) {
        return;
    }
    PLpgSQL_nsitem *temp_ns = expr->ns;
    while (temp_ns != NULL && strcmp((char *)temp_ns->name, "found") != 0) {
        if (strcmp(temp_ns->name, "") != 0) {
            char *default_val_query = "";
            if (((PLpgSQL_var*)(estate->datums[temp_ns->itemno]))->default_val != NULL) {
                default_val_query = ((PLpgSQL_var*)(estate->datums[temp_ns->itemno]))->default_val->query;
            }
            char * errMsg = (char*)palloc(sizeof(char)*128);
            bool ok = DeclareDrDBVarible(estate->handler,(char *)(temp_ns->name), ((PLpgSQL_var*)(estate->datums[temp_ns->itemno]))->datatype->typname, temp_ns->itemno, false, ((PLpgSQL_var*)(estate->datums[temp_ns->itemno]))->notnull, default_val_query, &errmsg);
            if (!ok) {
                ereport(ERROR, 
                            (errmsg("error happen when declare variable in go side")));
            }
            pfree(errMsg);
        }
        temp_ns = temp_ns->prev;
    }
}

/* ----------
 * Support function for initializing local execution variables
 * ----------
 */
static void
copy_plpgsql_datums(PLpgSQL_execstate *estate,
                    PLpgSQL_function *func)
{
    int ndatums = estate->ndatums;
    PLpgSQL_datum **indatums;
    PLpgSQL_datum **outdatums;
    char *workspace;
    char *ws_next;
    int i;

    /* Allocate local datum-pointer array */
    estate->datums = (PLpgSQL_datum **)
        palloc(sizeof(PLpgSQL_datum *) * ndatums);

    /*
	 * To reduce palloc overhead, we make a single palloc request for all the
	 * space needed for locally-instantiated datums.
	 */
    workspace = palloc(func->copiable_size);
    ws_next = workspace;

    /* Fill datum-pointer array, copying datums into workspace as needed */
    indatums = func->datums;
    outdatums = estate->datums;
    for (i = 0; i < ndatums; i++)
    {
        PLpgSQL_datum *indatum = indatums[i];
        PLpgSQL_datum *outdatum;

        /* This must agree with plpgsql_finish_datums on what is copiable */
        switch (indatum->dtype)
        {
        case PLPGSQL_DTYPE_VAR:
        case PLPGSQL_DTYPE_PROMISE:
            outdatum = (PLpgSQL_datum *)ws_next;
            memcpy(outdatum, indatum, sizeof(PLpgSQL_var));
            ws_next += MAXALIGN(sizeof(PLpgSQL_var));
            break;

        case PLPGSQL_DTYPE_REC:
            outdatum = (PLpgSQL_datum *)ws_next;
            memcpy(outdatum, indatum, sizeof(PLpgSQL_rec));
            ws_next += MAXALIGN(sizeof(PLpgSQL_rec));
            break;

        case PLPGSQL_DTYPE_ROW:
        case PLPGSQL_DTYPE_RECFIELD:
        case PLPGSQL_DTYPE_ARRAYELEM:

           /*
			* These datum records are read-only at runtime, so no need to
			* copy them (well, RECFIELD and ARRAYELEM contain cached
			* data, but we'd just as soon centralize the caching anyway).
			*/
            outdatum = indatum;
            break;

        default:
            elog(ERROR, "unrecognized dtype: %d", indatum->dtype);
            outdatum = NULL; /* keep compiler quiet */
            break;
        }

        outdatums[i] = outdatum;
    }

    Assert(ws_next == workspace + func->copiable_size);
}

/*
 * Create a memory context for statement-lifespan variables, if we don't
 * have one already.  It will be a child of stmt_mcontext_parent, which is
 * either the function's main context or a pushed-down outer stmt_mcontext.
 */
static MemoryContext
get_stmt_mcontext(PLpgSQL_execstate *estate)
{
    if (estate->stmt_mcontext == NULL)
    {
        estate->stmt_mcontext =
            AllocSetContextCreate(estate->stmt_mcontext_parent,
                                  "PLpgSQL per-statement data",
                                  ALLOCSET_DEFAULT_SIZES);
    }
    return estate->stmt_mcontext;
}

/*
 * Push down the current stmt_mcontext so that called statements won't use it.
 * This is needed by statements that have statement-lifespan data and need to
 * preserve it across some inner statements.  The caller should eventually do
 * pop_stmt_mcontext().
 */
static void
push_stmt_mcontext(PLpgSQL_execstate *estate)
{
    /* Should have done get_stmt_mcontext() first */
    Assert(estate->stmt_mcontext != NULL);
    /* Assert we've not messed up the stack linkage */
    Assert(MemoryContextGetParent(estate->stmt_mcontext) == estate->stmt_mcontext_parent);
    /* Push it down to become the parent of any nested stmt mcontext */
    estate->stmt_mcontext_parent = estate->stmt_mcontext;
    /* And make it not available for use directly */
    estate->stmt_mcontext = NULL;
}

/*
 * Undo push_stmt_mcontext().  We assume this is done just before or after
 * resetting the caller's stmt_mcontext; since that action will also delete
 * any child contexts, there's no need to explicitly delete whatever context
 * might currently be estate->stmt_mcontext.
 */
static void
pop_stmt_mcontext(PLpgSQL_execstate *estate)
{
    /* We need only pop the stack */
    estate->stmt_mcontext = estate->stmt_mcontext_parent;
    estate->stmt_mcontext_parent = MemoryContextGetParent(estate->stmt_mcontext);
}

/*
 * Subroutine for exec_stmt_block: does any condition in the condition list
 * match the current exception?
 */
static bool
exception_matches_conditions(ErrorData *edata, PLpgSQL_condition *cond)
{
    for (; cond != NULL; cond = cond->next)
    {
        int sqlerrstate = cond->sqlerrstate;

        /*
		 * OTHERS matches everything *except* query-canceled and
		 * assert-failure.  If you're foolish enough, you can match those
		 * explicitly.
		 */
        if (sqlerrstate == 0)
        {
            if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED &&
                edata->sqlerrcode != ERRCODE_ASSERT_FAILURE)
                return true;
        }
        /* Exact match? */
        else if (edata->sqlerrcode == sqlerrstate)
            return true;
        /* Category match? */
        else if (ERRCODE_IS_CATEGORY(sqlerrstate) &&
                 ERRCODE_TO_CATEGORY(edata->sqlerrcode) == sqlerrstate)
            return true;
    }
    return false;
}

/* ----------
 * exec_stmt_block			Execute a block of statements
 * ----------
 */
static int
exec_stmt_block(PLpgSQL_execstate *estate, PLpgSQL_stmt_block *block)
{
    volatile int rc = -1;
    int i;

    /*
	 * First initialize all variables declared in this block
	 */
    estate->err_text = gettext_noop("during statement block local variable initialization");

    for (i = 0; i < block->n_initvars; i++)
    {
        int n = block->initvarnos[i];
        PLpgSQL_datum *datum = estate->datums[n];

        /*
		 * The set of dtypes handled here must match plpgsql_add_initdatums().
		 *
		 * Note that we currently don't support promise datums within blocks,
		 * only at a function's outermost scope, so we needn't handle those
		 * here.
		 */
        switch (datum->dtype)
        {
        case PLPGSQL_DTYPE_VAR:
        {
            PLpgSQL_var *var = (PLpgSQL_var *)datum;

            /*
			 * Free any old value, in case re-entering block, and
			 * initialize to NULL
			 */
            assign_simple_var(estate, var, true, false);

            if (var->default_val == NULL)
            {
                /*
				 * If needed, give the datatype a chance to reject
				 * NULLs, by assigning a NULL to the variable.  We
				 * claim the value is of type UNKNOWN, not the var's
				 * datatype, else coercion will be skipped.
				 */
                if (var->datatype->typtype == TYPTYPE_DOMAIN)
                    exec_assign_value(estate,
                                      (PLpgSQL_datum *) var,
                                      (Datum) 0,
                                      true,
                                      UNKNOWNOID,
                                      -1, 0,n,0, false, -1);

                /* parser should have rejected NOT NULL */
                Assert(!var->notnull);
            }
            else
            {
                exec_assign_expr(estate, (PLpgSQL_datum *)var,
                                 var->default_val,i);
            }
        }
        break;

        case PLPGSQL_DTYPE_REC:
        {
            PLpgSQL_rec *rec = (PLpgSQL_rec *)datum;

            /*
					 * Deletion of any existing object will be handled during
					 * the assignments below, and in some cases it's more
					 * efficient for us not to get rid of it beforehand.
					 */
            if (rec->default_val == NULL)
            {
                /*
				 * If needed, give the datatype a chance to reject
				 * NULLs, by assigning a NULL to the variable.
				 */
                exec_move_row(estate, (PLpgSQL_variable *) rec,
                              NULL, NULL, 0);

                /* parser should have rejected NOT NULL */
                Assert(!rec->notnull);
            }
            else
            {
                exec_assign_expr(estate, (PLpgSQL_datum *)rec,
                                 rec->default_val,i);
            }
        }
        break;

        default:
            elog(ERROR, "unrecognized dtype: %d", datum->dtype);
        }
    }

    if (block->exceptions)
    {
        /*
		 * Execute the statements in the block's body inside a sub-transaction
		 */
        MemoryContext oldcontext = CurrentMemoryContext;
        ExprContext *old_eval_econtext = estate->eval_econtext;
        ErrorData *save_cur_error = estate->cur_error;
        MemoryContext stmt_mcontext;

        estate->err_text = gettext_noop("during statement block entry");

        /*
		 * We will need a stmt_mcontext to hold the error data if an error
		 * occurs.  It seems best to force it to exist before entering the
		 * subtransaction, so that we reduce the risk of out-of-memory during
		 * error recovery, and because this greatly simplifies restoring the
		 * stmt_mcontext stack to the correct state after an error.  We can
		 * ameliorate the cost of this by allowing the called statements to
		 * use this mcontext too; so we don't push it down here.
		 */
        stmt_mcontext = get_stmt_mcontext(estate);

        /* Want to run statements inside function's memory context */
        MemoryContextSwitchTo(oldcontext);

        PG_TRY();
        {
            /*
			 * We need to run the block's statements with a new eval_econtext
			 * that belongs to the current subtransaction; if we try to use
			 * the outer econtext then ExprContext shutdown callbacks will be
			 * called at the wrong times.
			 */
            plpgsql_create_econtext(estate);

            estate->err_text = NULL;

            /* Run the block's statements */
            rc = exec_stmts(estate, block->body);
            estate->err_text = gettext_noop("during statement block exit");
            /*
			 * If the block ended with RETURN, we may need to copy the return
			 * value out of the subtransaction eval_context.  We can avoid a
			 * physical copy if the value happens to be a R/W expanded object.
			 */
            MemoryContextSwitchTo(oldcontext);
            /* Assert that the stmt_mcontext stack is unchanged */
            Assert(stmt_mcontext == estate->stmt_mcontext);

            /*
			 * Revert to outer eval_econtext.  (The inner one was
			 * automatically cleaned up during subxact exit.)
			 */
            estate->eval_econtext = old_eval_econtext;
        }
        PG_CATCH();
        {
            ErrorData *edata;
            ListCell *e;

            estate->err_text = gettext_noop("during exception cleanup");

            /* Save error info in our stmt_mcontext */
            MemoryContextSwitchTo(stmt_mcontext);
            edata = CopyErrorData();
            /* Abort the inner transaction */
            MemoryContextSwitchTo(oldcontext);

            /*
			 * Set up the stmt_mcontext stack as though we had restored our
			 * previous state and then done push_stmt_mcontext().  The push is
			 * needed so that statements in the exception handler won't
			 * clobber the error data that's in our stmt_mcontext.
			 */
            estate->stmt_mcontext_parent = stmt_mcontext;
            estate->stmt_mcontext = NULL;

            /*
			 * Now we can delete any nested stmt_mcontexts that might have
			 * been created as children of ours.  (Note: we do not immediately
			 * release any statement-lifespan data that might have been left
			 * behind in stmt_mcontext itself.  We could attempt that by doing
			 * a MemoryContextReset on it before collecting the error data
			 * above, but it seems too risky to do any significant amount of
			 * work before collecting the error.)
			 */
            MemoryContextDeleteChildren(stmt_mcontext);

            /* Revert to outer eval_econtext */
            estate->eval_econtext = old_eval_econtext;

            /*
			 * Must clean up the econtext too.  However, any tuple table made
			 * in the subxact will have been thrown away by SPI during subxact
			 * abort, so we don't need to (and mustn't try to) free the
			 * eval_tuptable.
			 */
            exec_eval_cleanup(estate);

            /* Look for a matching exception handler */
            foreach (e, block->exceptions->exc_list)
            {
                PLpgSQL_exception *exception = (PLpgSQL_exception *)lfirst(e);

                if (exception_matches_conditions(edata, exception->conditions))
                {
                    /*
					 * Initialize the magic SQLSTATE and SQLERRM variables for
					 * the exception block; this also frees values from any
					 * prior use of the same exception. We needn't do this
					 * until we have found a matching exception.
					 */
                    PLpgSQL_var *state_var;
                    PLpgSQL_var *errm_var;

                    state_var = (PLpgSQL_var *)
                                    estate->datums[block->exceptions->sqlstate_varno];
                    errm_var = (PLpgSQL_var *)
                                   estate->datums[block->exceptions->sqlerrm_varno];

                    assign_text_var(estate, state_var, unpack_sql_state(edata->sqlerrcode));
                    assign_text_var(estate, errm_var, edata->message);

                    /*
					 * Also set up cur_error so the error data is accessible
					 * inside the handler.
					 */
                    estate->cur_error = edata;

                    estate->err_text = NULL;

                    rc = exec_stmts(estate, exception->action);

                    break;
                }
            }

            /*
			 * Restore previous state of cur_error, whether or not we executed
			 * a handler.  This is needed in case an error got thrown from
			 * some inner block's exception handler.
			 */
            estate->cur_error = save_cur_error;

            /* Restore stmt_mcontext stack and release the error data */
            pop_stmt_mcontext(estate);
            MemoryContextReset(stmt_mcontext);
        }
        PG_END_TRY();

        Assert(save_cur_error == estate->cur_error);
    }
    else
    {
        /*
		 * Just execute the statements in the block's body
		 */
        estate->err_text = NULL;

        rc = exec_stmts(estate, block->body);
    }

    estate->err_text = NULL;

    /*
	 * Handle the return code.  This is intentionally different from
	 * LOOP_RC_PROCESSING(): CONTINUE never matches a block, and EXIT matches
	 * a block only if there is a label match.
	 */
    switch (rc)
    {
    case PLPGSQL_RC_OK:
    case PLPGSQL_RC_RETURN:
    case PLPGSQL_RC_CONTINUE:
        return rc;

    case PLPGSQL_RC_EXIT:
        if (estate->exitlabel == NULL)
            return PLPGSQL_RC_EXIT;
        if (block->label == NULL)
            return PLPGSQL_RC_EXIT;
        if (strcmp(block->label, estate->exitlabel) != 0)
            return PLPGSQL_RC_EXIT;
        estate->exitlabel = NULL;
        return PLPGSQL_RC_OK;

    default:
        elog(ERROR, "unrecognized rc: %d", rc);
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmts			Iterate over a list of statements
 *				as long as their return code is OK
 * ----------
 */
static int
exec_stmts(PLpgSQL_execstate *estate, List *stmts)
{
    ListCell *s;

    if (stmts == NIL)
    {
        return PLPGSQL_RC_OK;
    }

    foreach (s, stmts)
    {
        PLpgSQL_stmt *stmt = (PLpgSQL_stmt *)lfirst(s);
        int rc = exec_stmt(estate, stmt);

        if (rc != PLPGSQL_RC_OK)
            return rc;
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt			Distribute one statement to the statements
 *				type specific execution function.
 * ----------
 */
static int
exec_stmt(PLpgSQL_execstate *estate, PLpgSQL_stmt *stmt)
{
    PLpgSQL_stmt *save_estmt;
    int rc = -1;

    save_estmt = estate->err_stmt;
    estate->err_stmt = stmt;

    switch (stmt->cmd_type)
    {
    case PLPGSQL_STMT_BLOCK:
        rc = exec_stmt_block(estate, (PLpgSQL_stmt_block *)stmt);
        break;

    case PLPGSQL_STMT_ASSIGN:
        rc = exec_stmt_assign(estate, (PLpgSQL_stmt_assign *)stmt);
        break;

    case PLPGSQL_STMT_PERFORM:
        rc = exec_stmt_perform(estate, (PLpgSQL_stmt_perform *)stmt);
        break;

    case PLPGSQL_STMT_CALL:
        rc = exec_stmt_call(estate, (PLpgSQL_stmt_call *)stmt);
        break;

    case PLPGSQL_STMT_GETDIAG:
        rc = exec_stmt_getdiag(estate, (PLpgSQL_stmt_getdiag *)stmt);
        break;

    case PLPGSQL_STMT_IF:
        rc = exec_stmt_if(estate, (PLpgSQL_stmt_if *)stmt);
        break;

    case PLPGSQL_STMT_CASE:
        rc = exec_stmt_case(estate, (PLpgSQL_stmt_case *)stmt);
        break;

    case PLPGSQL_STMT_LOOP:
        rc = exec_stmt_loop(estate, (PLpgSQL_stmt_loop *)stmt);
        break;

    case PLPGSQL_STMT_WHILE:
        rc = exec_stmt_while(estate, (PLpgSQL_stmt_while *)stmt);
        break;

    case PLPGSQL_STMT_FORI:
        rc = exec_stmt_fori(estate, (PLpgSQL_stmt_fori *)stmt);
        break;

    case PLPGSQL_STMT_FORS:
        rc = exec_stmt_fors(estate, (PLpgSQL_stmt_fors *)stmt);
        break;

    case PLPGSQL_STMT_FORC:
        rc = exec_stmt_forc(estate, (PLpgSQL_stmt_forc *)stmt);
        break;

    case PLPGSQL_STMT_FOREACH_A:
        rc = exec_stmt_foreach_a(estate, (PLpgSQL_stmt_foreach_a *)stmt);
        break;

    case PLPGSQL_STMT_EXIT:
        rc = exec_stmt_exit(estate, (PLpgSQL_stmt_exit *)stmt);
        break;

    case PLPGSQL_STMT_RETURN:
        rc = exec_stmt_return(estate, (PLpgSQL_stmt_return *)stmt);
        break;

    case PLPGSQL_STMT_RETURN_NEXT:
        rc = exec_stmt_return_next(estate, (PLpgSQL_stmt_return_next *)stmt);
        break;

    case PLPGSQL_STMT_RETURN_QUERY:
        rc = exec_stmt_return_query(estate, (PLpgSQL_stmt_return_query *)stmt);
        break;

    case PLPGSQL_STMT_RAISE:
        rc = exec_stmt_raise(estate, (PLpgSQL_stmt_raise *)stmt);
        break;

    case PLPGSQL_STMT_ASSERT:
        rc = exec_stmt_assert(estate, (PLpgSQL_stmt_assert *)stmt);
        break;

    case PLPGSQL_STMT_EXECSQL:
        rc = exec_stmt_execsql(estate, (PLpgSQL_stmt_execsql *)stmt);
        break;

    case PLPGSQL_STMT_DYNEXECUTE:
        rc = exec_stmt_dynexecute(estate, (PLpgSQL_stmt_dynexecute *)stmt);
        break;

    case PLPGSQL_STMT_DYNFORS:
        rc = exec_stmt_dynfors(estate, (PLpgSQL_stmt_dynfors *)stmt);
        break;

    case PLPGSQL_STMT_OPEN:
        rc = exec_stmt_open(estate, (PLpgSQL_stmt_open *)stmt);
        break;

    case PLPGSQL_STMT_FETCH:
        rc = exec_stmt_fetch(estate, (PLpgSQL_stmt_fetch *)stmt);
        break;

    case PLPGSQL_STMT_CLOSE:
        rc = exec_stmt_close(estate, (PLpgSQL_stmt_close *)stmt);
        break;

    case PLPGSQL_STMT_COMMIT:
        
        rc = exec_stmt_commit(estate, (PLpgSQL_stmt_commit *)stmt);
        break;

    case PLPGSQL_STMT_ROLLBACK:
        BEPI_TXN_rollback(estate->handler);
        break;

    case PLPGSQL_STMT_SET:
        rc = exec_stmt_set(estate, (PLpgSQL_stmt_set *)stmt);
        break;

    default:
        estate->err_stmt = save_estmt;
        elog(ERROR, "unrecognized cmd_type: %d", stmt->cmd_type);
    }
    estate->err_stmt = save_estmt;

    return rc;
}

/* ----------
 * exec_stmt_assign			Evaluate an expression and
 *					put the result into a variable.
 * ----------
 */
static int
exec_stmt_assign(PLpgSQL_execstate *estate, PLpgSQL_stmt_assign *stmt)
{
    Assert(stmt->varno >= 0);

    add_unvisited_variable_to_go(estate, stmt->expr);
    exec_assign_expr(estate,estate->datums[stmt->varno], stmt->expr,stmt->varno);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_perform		Evaluate query and discard result (but set
 *							FOUND depending on whether at least one row
 *							was returned).
 * ----------
 */
static int
exec_stmt_perform(PLpgSQL_execstate *estate, PLpgSQL_stmt_perform *stmt)
{
    PLpgSQL_expr *expr = stmt->expr;

    (void)exec_run_select(estate, expr, 0, NULL);
    exec_set_found(estate, (estate->eval_processed != 0));
    exec_eval_cleanup(estate);

    return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_call
 */
static int
exec_stmt_call(PLpgSQL_execstate *estate, PLpgSQL_stmt_call *stmt)
{
    ParamListInfo paramLI;
    long tcount = 0;
    bool is_query_or_hasReturning;
    PLpgSQL_expr* expr  = stmt->expr;
    exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK, true);
     paramLI = setup_param_list(estate, expr);
    int rc = BEPI_execute_plan_with_paramlist(estate->handler, expr->planIndex, paramLI,
                                          estate->readonly_func, tcount, &is_query_or_hasReturning);
    return rc;
}

/* ----------
 * exec_stmt_getdiag					Put internal PG information into
 *										specified variables.
 * ----------
 */
static int
exec_stmt_getdiag(PLpgSQL_execstate *estate, PLpgSQL_stmt_getdiag *stmt)
{
    ListCell *lc;

    /*
	 * GET STACKED DIAGNOSTICS is only valid inside an exception handler.
	 *
	 * Note: we trust the grammar to have disallowed the relevant item kinds
	 * if not is_stacked, otherwise we'd dump core below.
	 */
    if (stmt->is_stacked && estate->cur_error == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
                 errmsg("GET STACKED DIAGNOSTICS cannot be used outside an exception handler")));

    foreach (lc, stmt->diag_items)
    {
        PLpgSQL_diag_item *diag_item = (PLpgSQL_diag_item *)lfirst(lc);
        PLpgSQL_datum *var = estate->datums[diag_item->target];

        switch (diag_item->kind)
        {
        case PLPGSQL_GETDIAG_ROW_COUNT:
            exec_assign_value(estate, var,
                              UInt64GetDatum(estate->eval_processed),
                              false, INT8OID, -1, 0, diag_item->target, 0, false, -1);
            break;

        case PLPGSQL_GETDIAG_ERROR_CONTEXT:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->context);
            break;

        case PLPGSQL_GETDIAG_ERROR_DETAIL:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->detail);
            break;

        case PLPGSQL_GETDIAG_ERROR_HINT:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->hint);
            break;

        case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
            exec_assign_c_string(estate, var,
                                 NULL);
            break;

        case PLPGSQL_GETDIAG_COLUMN_NAME:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->column_name);
            break;

        case PLPGSQL_GETDIAG_CONSTRAINT_NAME:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->constraint_name);
            break;

        case PLPGSQL_GETDIAG_DATATYPE_NAME:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->datatype_name);
            break;

        case PLPGSQL_GETDIAG_MESSAGE_TEXT:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->message);
            break;

        case PLPGSQL_GETDIAG_TABLE_NAME:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->table_name);
            break;

        case PLPGSQL_GETDIAG_SCHEMA_NAME:
            exec_assign_c_string(estate, var,
                                 estate->cur_error->schema_name);
            break;

        case PLPGSQL_GETDIAG_CONTEXT:
        {
            char *contextstackstr;
            MemoryContext oldcontext;

            /* Use eval_mcontext for short-lived string */
            oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
            MemoryContextSwitchTo(oldcontext);

            exec_assign_c_string(estate, var, contextstackstr);
        }
        break;

        default:
            elog(ERROR, "unrecognized diagnostic item kind: %d",
                 diag_item->kind);
        }
    }

    exec_eval_cleanup(estate);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_if				Evaluate a bool expression and
 *					execute the true or false body
 *					conditionally.
 * ----------
 */
static int
exec_stmt_if(PLpgSQL_execstate *estate, PLpgSQL_stmt_if *stmt)
{
    bool value;
    bool isnull;
    ListCell *lc;

    add_unvisited_variable_to_go(estate, stmt->cond);
    exec_eval_cleanup(estate);
    value = exec_eval_boolean(estate, stmt->cond, &isnull);
    if (!isnull && value)
        return exec_stmts(estate, stmt->then_body);
    foreach (lc, stmt->elsif_list)
    {
        PLpgSQL_if_elsif *elif = (PLpgSQL_if_elsif *)lfirst(lc);

        value = exec_eval_boolean(estate, elif->cond, &isnull);
        exec_eval_cleanup(estate);
        if (!isnull && value)
            return exec_stmts(estate, elif->stmts);
    }
    return exec_stmts(estate, stmt->else_body);
}

/*
 *-----------
 * exec_stmt_case
 *-----------
 */
static int
exec_stmt_case(PLpgSQL_execstate *estate, PLpgSQL_stmt_case *stmt)
{
    PLpgSQL_var *t_var = NULL;
    bool isnull;
    ListCell *l;

    add_unvisited_variable_to_go(estate, stmt->t_expr);
    if (stmt->t_expr != NULL)
    {
        /* simple case */
        Datum t_val;
        Oid t_typoid;
        int32 t_typmod;

        t_val = exec_eval_expr(estate, stmt->t_expr, NULL,
                               &isnull, &t_typoid, &t_typmod);

        t_var = (PLpgSQL_var *)estate->datums[stmt->t_varno];

        /*
		 * When expected datatype is different from real, change it. Note that
		 * what we're modifying here is an execution copy of the datum, so
		 * this doesn't affect the originally stored function parse tree. (In
		 * theory, if the expression datatype keeps changing during execution,
		 * this could cause a function-lifespan memory leak.  Doesn't seem
		 * worth worrying about though.)
		 */
        if (t_var->datatype->typoid != t_typoid ||
            t_var->datatype->atttypmod != t_typmod)
            t_var->datatype = plpgsql_build_datatype("", t_typoid,
                                                     t_typmod,
                                                     estate->func->fn_input_collation);

        /* now we can assign to the variable */
        exec_assign_value(estate,
                          (PLpgSQL_datum *) t_var,
                          t_val,
                          isnull,
                          t_typoid,
                          t_typmod, 0,stmt->t_varno,0, false, -1);

        exec_eval_cleanup(estate);
    }

    /* Now search for a successful WHEN clause */
    foreach (l, stmt->case_when_list)
    {
        PLpgSQL_case_when *cwt = (PLpgSQL_case_when *)lfirst(l);
        bool value;

        value = exec_eval_boolean(estate, cwt->expr, &isnull);
        exec_eval_cleanup(estate);
        if (!isnull && value)
        {
            /* Found it */

            /* We can now discard any value we had for the temp variable */
            if (t_var != NULL)
                assign_simple_var(estate, t_var ,true, false);

            /* Evaluate the statement(s), and we're done */
            return exec_stmts(estate, cwt->stmts);
        }
    }

    /* We can now discard any value we had for the temp variable */
    if (t_var != NULL)
        assign_simple_var(estate, t_var, true, false);

    /* SQL2003 mandates this error if there was no ELSE clause */
    if (!stmt->have_else)
        ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND),
                 errmsg("case not found"),
                 errhint("CASE statement is missing ELSE part.")));

    /* Evaluate the ELSE statements, and we're done */
    return exec_stmts(estate, stmt->else_stmts);
}

/* ----------
 * exec_stmt_loop			Loop over statements until
 *					an exit occurs.
 * ----------
 */
static int
exec_stmt_loop(PLpgSQL_execstate *estate, PLpgSQL_stmt_loop *stmt)
{
    int rc = PLPGSQL_RC_OK;

    for (;;)
    {
        rc = exec_stmts(estate, stmt->body);

        LOOP_RC_PROCESSING(stmt->label, break);
    }

    return rc;
}

/* ----------
 * exec_stmt_while			Loop over statements as long
 *					as an expression evaluates to
 *					true or an exit occurs.
 * ----------
 */
static int
exec_stmt_while(PLpgSQL_execstate *estate, PLpgSQL_stmt_while *stmt)
{
    int rc = PLPGSQL_RC_OK;

    add_unvisited_variable_to_go(estate, stmt->cond);
    for (;;)
    {
        bool value;
        bool isnull;

        value = exec_eval_boolean(estate, stmt->cond, &isnull);
        exec_eval_cleanup(estate);

        if (isnull || !value)
            break;

        rc = exec_stmts(estate, stmt->body);

        LOOP_RC_PROCESSING(stmt->label, break);
    }

    return rc;
}

/* ----------
 * exec_stmt_fori			Iterate an integer variable
 *					from a lower to an upper value
 *					incrementing or decrementing by the BY value
 * ----------
 */
static int
exec_stmt_fori(PLpgSQL_execstate *estate, PLpgSQL_stmt_fori *stmt)
{
    PLpgSQL_var *var;
    bool isnull;
    Oid valtype;
    int32 valtypmod;
    int32 loop_value;
    int32 end_value;
    int32 step_value;
    bool found = false;
    int rc = PLPGSQL_RC_OK;
    char *err = palloc(ERRSIZE);
    memset(err, 0, ERRSIZE);

    var = (PLpgSQL_var *)(estate->datums[stmt->var->dno]);

    /*
	 * Get the value of the lower bound
	 */
    exec_eval_expr(estate, stmt->lower, NULL,
                           &isnull, &valtype, &valtypmod);
    if (isnull)
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("lower bound of FOR loop cannot be null")));
    // get value from udrRunParams.result
    loop_value = (int32)GetIntFromResult(estate->handler, &err);
    if (strlen(err) != 0) {
        ereport(ERROR,
                (errmsg("%s", err)));
    }

    exec_eval_cleanup(estate);

    /*
	 * Get the value of the upper bound
	 */
    exec_eval_expr(estate, stmt->upper, NULL,
                           &isnull, &valtype, &valtypmod);
    if (isnull)
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("upper bound of FOR loop cannot be null")));
    // get value from udrRunParams.result
    end_value = (int32)GetIntFromResult(estate->handler, &err);
    if (strlen(err) != 0) {
        ereport(ERROR,
                (errmsg("%s", err)));
    }
    exec_eval_cleanup(estate);

    /*
	 * Get the step value
	 */
    if (stmt->step)
    {
        exec_eval_expr(estate, stmt->step, NULL,
                               &isnull, &valtype, &valtypmod);
        if (isnull)
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("BY value of FOR loop cannot be null")));
        step_value = (int32)GetIntFromResult(estate->handler, &err);
        if (strlen(err) != 0) {
            ereport(ERROR,
                    (errmsg("%s", err)));
        }
        exec_eval_cleanup(estate);
        if (step_value <= 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("BY value of FOR loop must be greater than zero")));
        }
    }
    else
        step_value = 1;
    /*
	 * Now do the loop
	 */
    for (;;)
    {
        /*
		 * Check against upper bound
		 */
        if (stmt->reverse)
        {
            if (loop_value < end_value)
                break;
        }
        else
        {
            if (loop_value > end_value)
                break;
        }

        found = true; /* looped at least once */


        BepiSetResultInt(estate->handler,loop_value);
        /*
		 * Assign current value to loop var
		 */
        AssignTheValue(estate->handler,stmt->var->dno,0,false, false, -1);
        assign_simple_var(estate, var, false, false);

        /*
		 * Execute the statements
		 */
        rc = exec_stmts(estate, stmt->body);

        LOOP_RC_PROCESSING(stmt->label, break);

        /*
		 * Increase/decrease loop value, unless it would overflow, in which
		 * case exit the loop.
		 */
        if (stmt->reverse)
        {
            if (loop_value < (PG_INT32_MIN + step_value))
                break;
            loop_value -= step_value;
        }
        else
        {
            if (loop_value > (PG_INT32_MAX - step_value))
                break;
            loop_value += step_value;
        }
    }

    /*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set here so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
    exec_set_found(estate, found);

    return rc;
}

/* ----------
 * exec_stmt_fors			Execute a query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int
exec_stmt_fors(PLpgSQL_execstate *estate, PLpgSQL_stmt_fors *stmt)
{
    Portal portal;
    int rc;

    /*
	 * Open the implicit cursor for the statement using exec_run_select
	 */
    exec_run_select(estate, stmt->query, 0, &portal);

    /*
	 * Execute the loop
	 */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq *)stmt, portal, true);

    /*
	 * Close the implicit cursor
	 */
    SPI_cursor_close(portal);

    return rc;
}

/* ----------
 * exec_stmt_forc			Execute a loop for each row from a cursor.
 * ----------
 */
static int
exec_stmt_forc(PLpgSQL_execstate *estate, PLpgSQL_stmt_forc *stmt)
{
    PLpgSQL_var *curvar;
    MemoryContext stmt_mcontext = NULL;
    char *curname = NULL;
    PLpgSQL_expr *query;
    ParamListInfo paramLI;
    Portal portal;
    int rc;

    /* ----------
	 * Get the cursor variable and if it has an assigned name, check
	 * that it's not in use currently.
	 * ----------
	 */
    curvar = (PLpgSQL_var *)(estate->datums[stmt->curvar]);
    if (!curvar->isnull)
    {
        MemoryContext oldcontext;

        /* We only need stmt_mcontext to hold the cursor name string */
        stmt_mcontext = get_stmt_mcontext(estate);
        oldcontext = MemoryContextSwitchTo(stmt_mcontext);
        MemoryContextSwitchTo(oldcontext);

        if (SPI_cursor_find(curname) != NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_CURSOR),
                     errmsg("cursor \"%s\" already in use", curname)));
    }

    /* ----------
	 * Open the cursor just like an OPEN command
	 *
	 * Note: parser should already have checked that statement supplies
	 * args iff cursor needs them, but we check again to be safe.
	 * ----------
	 */
    if (stmt->argquery != NULL)
    {
        /* ----------
		 * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
		 * statement to evaluate the args and put 'em into the
		 * internal row.
		 * ----------
		 */
        PLpgSQL_stmt_execsql set_args;

        if (curvar->cursor_explicit_argrow < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("arguments given for cursor without arguments")));

        memset(&set_args, 0, sizeof(set_args));
        set_args.cmd_type = PLPGSQL_STMT_EXECSQL;
        set_args.lineno = stmt->lineno;
        set_args.sqlstmt = stmt->argquery;
        set_args.into = true;
        /* XXX historically this has not been STRICT */
        set_args.target = (PLpgSQL_variable *)(estate->datums[curvar->cursor_explicit_argrow]);

        if (exec_stmt_execsql(estate, &set_args) != PLPGSQL_RC_OK)
            elog(ERROR, "open cursor failed during argument processing");
    }
    else
    {
        if (curvar->cursor_explicit_argrow >= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("arguments required for cursor")));
    }

    query = curvar->cursor_explicit_expr;
    Assert(query);

    if (query->plan == NULL)
        exec_prepare_plan(estate, query, curvar->cursor_options, true);

    /*
	 * Set up ParamListInfo for this query
	 */
    paramLI = setup_param_list(estate, query);

    /*
	 * Open the cursor (the paramlist will get copied into the portal)
	 */
    portal = SPI_cursor_open_with_paramlist(curname, query->plan,
                                            paramLI,
                                            estate->readonly_func);
    if (portal == NULL)
        elog(ERROR, "could not open cursor: %s",
             SPI_result_code_string(0));

    /*
	 * If cursor variable was NULL, store the generated portal name in it
	 */
    if (curname == NULL)
        assign_text_var(estate, curvar, portal->name);

    /*
	 * Clean up before entering exec_for_query
	 */
    exec_eval_cleanup(estate);
    if (stmt_mcontext)
        MemoryContextReset(stmt_mcontext);

    /*
	 * Execute the loop.  We can't prefetch because the cursor is accessible
	 * to the user, for instance via UPDATE WHERE CURRENT OF within the loop.
	 */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq *)stmt, portal, false);

    /* ----------
	 * Close portal, and restore cursor variable if it was initially NULL.
	 * ----------
	 */
    SPI_cursor_close(portal);

    if (curname == NULL)
        assign_simple_var(estate, curvar, true, false);

    return rc;
}

/* ----------
 * exec_stmt_foreach_a			Loop over elements or slices of an array
 *
 * When looping over elements, the loop variable is the same type that the
 * array stores (eg: integer), when looping through slices, the loop variable
 * is an array of size and dimensions to match the size of the slice.
 * ----------
 */
static int
exec_stmt_foreach_a(PLpgSQL_execstate *estate, PLpgSQL_stmt_foreach_a *stmt)
{
    Oid arrtype;
    int32 arrtypmod;
    PLpgSQL_datum *loop_var;
    Oid loop_var_elem_type;
    bool found = false;
    int rc = PLPGSQL_RC_OK;
    MemoryContext stmt_mcontext;
    MemoryContext oldcontext;
    Oid iterator_result_type;
    int32 iterator_result_typmod;
    bool isnull;

    /* get the value of the array expression */
    exec_eval_expr(estate, stmt->expr, NULL, &isnull, &arrtype, &arrtypmod);
    /*
	 * Do as much as possible of the code below in stmt_mcontext, to avoid any
	 * leaks from called subroutines.  We need a private stmt_mcontext since
	 * we'll be calling arbitrary statement code.
	 */
    stmt_mcontext = get_stmt_mcontext(estate);
    push_stmt_mcontext(estate);
    oldcontext = MemoryContextSwitchTo(stmt_mcontext);
    /* Clean up any leftover temporary memory */
    exec_eval_cleanup(estate);
    /* Set up the loop variable and see if it is of an array type */
    loop_var = estate->datums[stmt->varno];
    if (loop_var->dtype == PLPGSQL_DTYPE_REC ||
        loop_var->dtype == PLPGSQL_DTYPE_ROW)
    {
        /*
		 * Record/row variable is certainly not of array type, and might not
		 * be initialized at all yet, so don't try to get its type
		 */
        loop_var_elem_type = InvalidOid;
    }
    else
        //TODO(zyk):增加类型返回
        loop_var_elem_type = InvalidOid;
    int len = BepiGetResultLen(estate->handler);
    /*
	 * Sanity-check the loop variable type.  We don't try very hard here, and
	 * should not be too picky since it's possible that exec_assign_value can
	 * coerce values of different types.  But it seems worthwhile to complain
	 * if the array-ness of the loop variable is not right.
	 */
    if (stmt->slice > 0 && loop_var_elem_type == InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("FOREACH ... SLICE loop variable must be of an array type")));
    if (stmt->slice == 0 && loop_var_elem_type != InvalidOid)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("FOREACH loop variable must not be of an array type")));
    /* Identify iterator result type */
    if (stmt->slice > 0)
    {
        /* When slicing, nominal type of result is same as array type */
        iterator_result_type = arrtype;
        iterator_result_typmod = arrtypmod;
    }
    else
    {
        /* Without slicing, results are individual array elements */
        iterator_result_typmod = arrtypmod;
    }
    int begin = 0;
    /* Iterate over the array elements or slices */
    while (begin < len)
    {
        
        found = true; /* looped at least once */
        // eval expr to refresh the udrRunParams.result to get the correct result
        exec_eval_expr(estate, stmt->expr, NULL, &isnull, &arrtype, &arrtypmod);
        /* exec_assign_value and exec_stmts must run in the main context */
        MemoryContextSwitchTo(oldcontext);
        /* Assign current element/slice to the loop variable */
        Datum value = (Datum)0;
        exec_assign_value(estate, loop_var, value, isnull,
                          iterator_result_type, iterator_result_typmod, 0, stmt->varno, 0, true, begin);
        begin++;
        /* In slice case, value is temporary; must free it to avoid leakage */
        if (stmt->slice > 0)
            pfree(DatumGetPointer(value));
        /*
		 * Execute the statements
		 */
        rc = exec_stmts(estate, stmt->body);
        LOOP_RC_PROCESSING(stmt->label, break);
        MemoryContextSwitchTo(stmt_mcontext);
    }
    /* Restore memory context state */
    MemoryContextSwitchTo(oldcontext);
    pop_stmt_mcontext(estate);
    /* Release temporary memory, including the array value */
    MemoryContextReset(stmt_mcontext);
    /*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set here so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
    exec_set_found(estate, found);
    return rc;
}

/* ----------
 * exec_stmt_exit			Implements EXIT and CONTINUE
 *
 * This begins the process of exiting / restarting a loop.
 * ----------
 */
static int
exec_stmt_exit(PLpgSQL_execstate *estate, PLpgSQL_stmt_exit *stmt)
{
    /*
	 * If the exit / continue has a condition, evaluate it
	 */
    if (stmt->cond != NULL)
    {
        bool value;
        bool isnull;

        add_unvisited_variable_to_go(estate, stmt->cond);
        value = exec_eval_boolean(estate, stmt->cond, &isnull);
        exec_eval_cleanup(estate);
        if (isnull || value == false)
            return PLPGSQL_RC_OK;
    }

    estate->exitlabel = stmt->label;
    if (stmt->is_exit)
        return PLPGSQL_RC_EXIT;
    else
        return PLPGSQL_RC_CONTINUE;
}
/* ----------
 * exec_stmt_return			Evaluate an expression and start
 *					returning from the function.
 *
 * Note: The result may be in the eval_mcontext.  Therefore, we must not
 * do exec_eval_cleanup while unwinding the control stack.
 * ----------
 */
static int
exec_stmt_return(PLpgSQL_execstate *estate, PLpgSQL_stmt_return *stmt)
{
    /*
	 * If processing a set-returning PL/pgSQL function, the final RETURN
	 * indicates that the function is finished producing tuples.  The rest of
	 * the work will be done at the top level.
	 */
    if (estate->retisset || estate->func == NULL || estate->func->out_param_varno != -1)
        return PLPGSQL_RC_RETURN;

    /* initialize for null result */
    estate->retval = (Datum)0;
    estate->retisnull = true;
    estate->rettype = InvalidOid;
    // visit expr->ns to add unbuilt variable to datum list in GO side or add alias name for exist variable
    add_unvisited_variable_to_go(estate, stmt->expr);
    /*
	 * Special case path when the RETURN expression is a simple variable
	 * reference; in particular, this path is always taken in functions with
	 * one or more OUT parameters.
	 *
	 * This special case is especially efficient for returning variables that
	 * have R/W expanded values: we can put the R/W pointer directly into
	 * estate->retval, leading to transferring the value to the caller's
	 * context cheaply.  If we went through exec_eval_expr we'd end up with a
	 * R/O pointer.  It's okay to skip MakeExpandedObjectReadOnly here since
	 * we know we won't need the variable's value within the function anymore.
	 */
	 char *sql_str = "";
	 if (stmt->expr != NULL)
	 {
	    sql_str = stmt->expr->query;
	 }
    if (stmt->retvarno >= 0)
    {
        PLpgSQL_datum *retvar = estate->datums[stmt->retvarno];
        BepiExecReturnAndSet(estate->handler, sql_str, stmt->retvarno);
        switch (retvar->dtype)
        {
        case PLPGSQL_DTYPE_PROMISE:

        case PLPGSQL_DTYPE_VAR:
        {
            PLpgSQL_var *var = (PLpgSQL_var *)retvar;

            estate->retval = var->value;
            estate->retisnull = var->isnull;
            estate->rettype = var->datatype->typoid;
            /*
			 * A PLpgSQL_var could not be of composite type, so
			 * conversion must fail if retistuple.  We throw a custom
			 * error mainly for consistency with historical behavior.
			 * For the same reason, we don't throw error if the result
			 * is NULL.  (Note that plpgsql_exec_trigger assumes that
			 * any non-null result has been verified to be composite.)
			 */
            if (estate->retistuple && !estate->retisnull)
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("cannot return non-composite value from function returning composite type")));
        }
        break;
        case PLPGSQL_DTYPE_REC:
        {
            PLpgSQL_rec *rec = (PLpgSQL_rec *)retvar;

            /* If record is empty, we return NULL not a row of nulls */
            if (rec->erh)
            {
                estate->retisnull = false;
                estate->rettype = rec->rectypeid;
            }
        }
        break;
        case PLPGSQL_DTYPE_ROW:
        {
            PLpgSQL_row *row = (PLpgSQL_row *)retvar;
            int32 rettypmod;

            /* We get here if there are multiple OUT parameters */
            exec_eval_datum(estate,
                            (PLpgSQL_datum *)row,
                            &estate->rettype,
                            &rettypmod,
                            &estate->retval,
                            &estate->retisnull);
        }
        break;

        default:
            elog(ERROR, "unrecognized dtype: %d", retvar->dtype);
        }

        return PLPGSQL_RC_RETURN;
    }

    if (stmt->expr != NULL)
    {
        int32 rettypmod;

        estate->retval = exec_eval_expr(estate, stmt->expr, NULL,
                                        &(estate->retisnull),
                                        &(estate->rettype),
                                        &rettypmod);
        char* errMsg = BepiExecReturnAndSet(estate->handler, sql_str, -1);
        if (errMsg != NULL) {
            if (strlen(errMsg) != 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                                 errmsg(errMsg)));
            }
        }

        /*
		 * As in the DTYPE_VAR case above, throw a custom error if a non-null,
		 * non-composite value is returned in a function returning tuple.
		 */

        return PLPGSQL_RC_RETURN;
    }

    /*
	 * Special hack for function returning VOID: instead of NULL, return a
	 * non-null VOID value.  This is of dubious importance but is kept for
	 * backwards compatibility.  We don't do it for procedures, though.
	 */
    if (estate->fn_rettype == VOIDOID &&
        estate->func->fn_prokind != PROKIND_PROCEDURE)
    {
        estate->retval = (Datum)0;
        estate->retisnull = false;
        estate->rettype = VOIDOID;
    }

    return PLPGSQL_RC_RETURN;
}

/* ----------
 * exec_stmt_return_next		Evaluate an expression and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int
exec_stmt_return_next(PLpgSQL_execstate *estate,
                      PLpgSQL_stmt_return_next *stmt)
{
    TupleDesc tupdesc;
    int natts;
    HeapTuple tuple;
    MemoryContext oldcontext;

    if (!estate->retisset)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("cannot use RETURN NEXT in a non-SETOF function")));

    /* tuple_store_desc will be filled by exec_init_tuple_store */
    tupdesc = estate->tuple_store_desc;
    natts = tupdesc->natts;

    /*
	 * Special case path when the RETURN NEXT expression is a simple variable
	 * reference; in particular, this path is always taken in functions with
	 * one or more OUT parameters.
	 *
	 * Unlike exec_stmt_return, there's no special win here for R/W expanded
	 * values, since they'll have to get flattened to go into the tuplestore.
	 * Indeed, we'd better make them R/O to avoid any risk of the casting step
	 * changing them in-place.
	 */
    if (stmt->retvarno >= 0)
    {
        PLpgSQL_datum *retvar = estate->datums[stmt->retvarno];

        switch (retvar->dtype)
        {
        case PLPGSQL_DTYPE_PROMISE:

        case PLPGSQL_DTYPE_VAR:
        {
            PLpgSQL_var *var = (PLpgSQL_var *)retvar;
            Datum retval = var->value;
            bool isNull = var->isnull;
            Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);

            if (natts != 1)
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("wrong result type supplied in RETURN NEXT")));

            /* coerce type if needed */
            retval = exec_cast_value(estate,
                                     retval,
                                     &isNull,
                                     var->datatype->typoid,
                                     var->datatype->atttypmod,
                                     attr->atttypid,
                                     attr->atttypmod);

        }
        break;

        case PLPGSQL_DTYPE_REC:
        {
            PLpgSQL_rec *rec = (PLpgSQL_rec *)retvar;
            // TupleDesc rec_tupdesc;
            /* If rec is null, try to convert it to a row of nulls */
            if (rec->erh == NULL)
                instantiate_empty_record_variable(estate, rec);
        }
        break;

        case PLPGSQL_DTYPE_ROW:
        {
            PLpgSQL_row *row = (PLpgSQL_row *)retvar;
            oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
            tuple = make_tuple_from_row(estate, row, tupdesc);
            if (tuple == NULL) /* should not happen */
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("wrong record type supplied in RETURN NEXT")));
            MemoryContextSwitchTo(oldcontext);
        }
        break;

        default:
            elog(ERROR, "unrecognized dtype: %d", retvar->dtype);
            break;
        }
    }
    else if (stmt->expr)
    {
        Datum retval;
        bool isNull;
        Oid rettype;
        int32 rettypmod;

        retval = exec_eval_expr(estate,
                                stmt->expr, NULL,
                                &isNull,
                                &rettype,
                                &rettypmod);

        if (estate->retistuple)
        {
            /* Expression should be of RECORD or composite type */
            if (!isNull)
            {
                HeapTupleData tmptup;

                /* Use eval_mcontext for tuple conversion work */
                oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
                tuple = &tmptup;
                MemoryContextSwitchTo(oldcontext);
            }
            else
            {
                /* Composite NULL --- store a row of nulls */
                Datum *nulldatums;
                bool *nullflags;

                nulldatums = (Datum *)
                    eval_mcontext_alloc0(estate, natts * sizeof(Datum));
                nullflags = (bool *)
                    eval_mcontext_alloc(estate, natts * sizeof(bool));
                memset(nullflags, true, natts * sizeof(bool));
            }
        }
        else
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);

            /* Simple scalar result */
            if (natts != 1)
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("wrong result type supplied in RETURN NEXT")));

            /* coerce type if needed */
            retval = exec_cast_value(estate,
                                     retval,
                                     &isNull,
                                     rettype,
                                     rettypmod,
                                     attr->atttypid,
                                     attr->atttypmod);
        }
    }
    else
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("RETURN NEXT must have a parameter")));
    }

    exec_eval_cleanup(estate);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_return_query		Evaluate a query and add it to the
 *								list of tuples returned by the current
 *								SRF.
 * ----------
 */
static int
exec_stmt_return_query(PLpgSQL_execstate *estate,
                       PLpgSQL_stmt_return_query *stmt)
{
    Portal portal;
    uint64 processed = 0;
    MemoryContext oldcontext;

    if (!estate->retisset)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("cannot use RETURN QUERY in a non-SETOF function")));
    if (stmt->query != NULL)
    {
        /* static query */
        exec_run_select(estate, stmt->query, 0, &portal);
    }
    else
    {
        /* RETURN QUERY EXECUTE */
        Assert(stmt->dynquery != NULL);
        portal = exec_dynquery_with_params(estate, stmt->dynquery,
                                           stmt->params, NULL,
                                           0);
    }
    /* Use eval_mcontext for tuple conversion work */
    oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
    while (true)
    {
        uint64 i;

        SPI_cursor_fetch(portal, true, 0);

        /* SPI will have changed CurrentMemoryContext */
        MemoryContextSwitchTo(get_eval_mcontext(estate));

        if (SPI_processed == 0)
            break;

        for (i = 0; i < SPI_processed; i++)
        {
            processed++;
        }
        SPI_freetuptable(SPI_tuptable);
    }

    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);

    MemoryContextSwitchTo(oldcontext);
    exec_eval_cleanup(estate);

    estate->eval_processed = processed;
    exec_set_found(estate, processed != 0);

    return PLPGSQL_RC_OK;
}

#define SET_RAISE_OPTION_TEXT(opt, name)                          \
    do                                                            \
    {                                                             \
        if (opt)                                                  \
            ereport(ERROR,                                        \
                    (errcode(ERRCODE_SYNTAX_ERROR),               \
                     errmsg("RAISE option already specified: %s", \
                            name)));                              \
        opt = MemoryContextStrdup(stmt_mcontext, extval);         \
    } while (0)

/* ----------
 * exec_stmt_raise			Build a message and throw it with elog()
 * ----------
 */
static int
exec_stmt_raise(PLpgSQL_execstate *estate, PLpgSQL_stmt_raise *stmt)
{
    int err_code = 0;
    char *condname = NULL;
    char *err_message = NULL;
    char *err_detail = NULL;
    char *err_hint = NULL;
    char *err_column = NULL;
    char *err_constraint = NULL;
    char *err_datatype = NULL;
    char *err_table = NULL;
    char *err_schema = NULL;
    MemoryContext stmt_mcontext;
    ListCell *lc;

    /* RAISE with no parameters: re-throw current exception */
    if (stmt->condname == NULL && stmt->message == NULL &&
        stmt->options == NIL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER),
                 errmsg("RAISE without parameters cannot be used outside an exception handler")));
    }

    /* We'll need to accumulate the various strings in stmt_mcontext */
    stmt_mcontext = get_stmt_mcontext(estate);

    if (stmt->condname)
    {
        err_code = plpgsql_recognize_err_condition(stmt->condname, true);
        condname = MemoryContextStrdup(stmt_mcontext, stmt->condname);
    }

    if (stmt->message)
    {
        StringInfoData ds;
        ListCell *current_param;
        char *cp;
        MemoryContext oldcontext;

        /* build string in stmt_mcontext */
        oldcontext = MemoryContextSwitchTo(stmt_mcontext);
        initStringInfo(&ds);
        MemoryContextSwitchTo(oldcontext);

        current_param = list_head(stmt->params);

        for (cp = stmt->message; *cp; cp++)
        {
            /*
			 * Occurrences of a single % are replaced by the next parameter's
			 * external representation. Double %'s are converted to one %.
			 */
            if (cp[0] == '%')
            {
                Oid paramtypeid;
                int32 paramtypmod;
                Datum paramvalue;
                bool paramisnull;
                char *extval;

                if (cp[1] == '%')
                {
                    appendStringInfoChar(&ds, '%');
                    cp++;
                    continue;
                }

                /* should have been checked at compile time */
                if (current_param == NULL)
                    elog(ERROR, "unexpected RAISE parameter list length");

                paramvalue = exec_eval_expr(estate,
                                            (PLpgSQL_expr *) lfirst(current_param), NULL,
                                            &paramisnull,
                                            &paramtypeid,
                                            &paramtypmod);

                if (paramisnull)
                    extval = "<NULL>";
                else
                    extval = convert_value_to_string(estate,
                                                     paramvalue,
                                                     paramtypeid);
                appendStringInfoString(&ds, extval);
                current_param = lnext(stmt->params, current_param);
                exec_eval_cleanup(estate);
            }
            else
                appendStringInfoChar(&ds, cp[0]);
        }

        /* should have been checked at compile time */
        if (current_param != NULL)
            elog(ERROR, "unexpected RAISE parameter list length");

        err_message = ds.data;
    }

    foreach (lc, stmt->options)
    {
        PLpgSQL_raise_option *opt = (PLpgSQL_raise_option *)lfirst(lc);
        Datum optionvalue;
        bool optionisnull;
        Oid optiontypeid;
        int32 optiontypmod;
        char *extval;

        optionvalue = exec_eval_expr(estate, opt->expr, NULL,
                                     &optionisnull,
                                     &optiontypeid,
                                     &optiontypmod);
        if (optionisnull)
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("RAISE statement option cannot be null")));

        extval = convert_value_to_string(estate, optionvalue, optiontypeid);

        switch (opt->opt_type)
        {
        case PLPGSQL_RAISEOPTION_ERRCODE:
            if (err_code)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("RAISE option already specified: %s",
                                "ERRCODE")));
            err_code = plpgsql_recognize_err_condition(extval, true);
            condname = MemoryContextStrdup(stmt_mcontext, extval);
            break;
        case PLPGSQL_RAISEOPTION_MESSAGE:
            SET_RAISE_OPTION_TEXT(err_message, "MESSAGE");
            break;
        case PLPGSQL_RAISEOPTION_DETAIL:
            SET_RAISE_OPTION_TEXT(err_detail, "DETAIL");
            break;
        case PLPGSQL_RAISEOPTION_HINT:
            SET_RAISE_OPTION_TEXT(err_hint, "HINT");
            break;
        case PLPGSQL_RAISEOPTION_COLUMN:
            SET_RAISE_OPTION_TEXT(err_column, "COLUMN");
            break;
        case PLPGSQL_RAISEOPTION_CONSTRAINT:
            SET_RAISE_OPTION_TEXT(err_constraint, "CONSTRAINT");
            break;
        case PLPGSQL_RAISEOPTION_DATATYPE:
            SET_RAISE_OPTION_TEXT(err_datatype, "DATATYPE");
            break;
        case PLPGSQL_RAISEOPTION_TABLE:
            SET_RAISE_OPTION_TEXT(err_table, "TABLE");
            break;
        case PLPGSQL_RAISEOPTION_SCHEMA:
            SET_RAISE_OPTION_TEXT(err_schema, "SCHEMA");
            break;
        default:
            elog(ERROR, "unrecognized raise option: %d", opt->opt_type);
        }

        exec_eval_cleanup(estate);
    }

    /* Default code if nothing specified */
    if (err_code == 0 && stmt->elog_level >= ERROR)
        err_code = ERRCODE_RAISE_EXCEPTION;

    /* Default error message if nothing specified */
    if (err_message == NULL)
    {
        if (condname)
        {
            err_message = condname;
            condname = NULL;
        }
        else
            err_message = MemoryContextStrdup(stmt_mcontext, unpack_sql_state(err_code));
    }

    /*
	 * Throw the error (may or may not come back)
	 */
    ereport(stmt->elog_level,
    (err_code ? errcode(err_code) : 0,
    errmsg_internal("%s", err_message),
    (err_detail != NULL) ? errdetail_internal("%s", err_detail) : 0,
    (err_hint != NULL) ? errhint("%s", err_hint) : 0,
    (err_column != NULL) ?
    err_generic_string(PG_DIAG_COLUMN_NAME, err_column) : 0,
    (err_constraint != NULL) ?
    err_generic_string(PG_DIAG_CONSTRAINT_NAME, err_constraint) : 0,
    (err_datatype != NULL) ?
    err_generic_string(PG_DIAG_DATATYPE_NAME, err_datatype) : 0,
    (err_table != NULL) ?
    err_generic_string(PG_DIAG_TABLE_NAME, err_table) : 0,
    (err_schema != NULL) ?
    err_generic_string(PG_DIAG_SCHEMA_NAME, err_schema) : 0));

    /* Clean up transient strings */
    MemoryContextReset(stmt_mcontext);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_assert			Assert statement
 * ----------
 */
static int
exec_stmt_assert(PLpgSQL_execstate *estate, PLpgSQL_stmt_assert *stmt)
{
    bool value;
    bool isnull;

    /* do nothing when asserts are not enabled */
    if (!plpgsql_check_asserts)
        return PLPGSQL_RC_OK;

    value = exec_eval_boolean(estate, stmt->cond, &isnull);
    exec_eval_cleanup(estate);
    if (isnull || !value)
    {
        if (stmt->message != NULL)
        {
            Datum val;
            Oid typeid;
            int32 typmod;

            val = exec_eval_expr(estate, stmt->message, NULL,
                                 &isnull, &typeid, &typmod);
        }
        ereport(ERROR,
                (errcode(ERRCODE_ASSERT_FAILURE),
                 errmsg("assertion failed")));
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * Initialize a mostly empty execution state
 * ----------
 */
static void
plpgsql_estate_setup(PLpgSQL_execstate *estate,
                     PLpgSQL_function *func,
                     ReturnSetInfo *rsi,
                     EState *simple_eval_estate)
{
    /* this link will be restored at exit from plpgsql_call_handler */
    func->cur_estate = estate;

    estate->func = func;
    estate->trigdata = NULL;

    estate->retval = (Datum)0;
    estate->retisnull = true;
    estate->rettype = InvalidOid;

    estate->fn_rettype = func->fn_rettype;
    estate->retistuple = func->fn_retistuple;
    estate->retisset = func->fn_retset;

    estate->readonly_func = func->fn_readonly;
    estate->atomic = true;

    estate->exitlabel = NULL;
    estate->cur_error = NULL;

    /*estate->tuple_store = NULL;*/
    estate->tuple_store_desc = NULL;
    if (!rsi)estate->tuple_store_cxt = NULL;
    estate->rsi = rsi;

    estate->found_varno = func->found_varno;
    estate->ndatums = func->ndatums;
    estate->datums = NULL;
    /* the datums array will be filled by copy_plpgsql_datums() */
    estate->datum_context = CurrentMemoryContext;

    /* set up for use of appropriate simple-expression EState and cast hash */
    if (simple_eval_estate)
    {
        estate->simple_eval_estate = simple_eval_estate;
        estate->cast_hash_context = CurrentMemoryContext;
    }
    else
    {
        estate->simple_eval_estate = shared_simple_eval_estate;
        //移除cast hash
    }

    /*
	 * We start with no stmt_mcontext; one will be created only if needed.
	 * That context will be a direct child of the function's main execution
	 * context.  Additional stmt_mcontexts might be created as children of it.
	 */
    estate->stmt_mcontext = NULL;
    estate->stmt_mcontext_parent = CurrentMemoryContext;

    estate->eval_tuptable = NULL;
    estate->eval_processed = 0;
    estate->eval_econtext = NULL;

    estate->err_stmt = NULL;
    estate->err_text = NULL;

    estate->plugin_info = NULL;

    /*
	 * Create an EState and ExprContext for evaluation of simple expressions.
	 */
    plpgsql_create_econtext(estate);

}

/* ----------
 * Release temporary memory used by expression/subselect evaluation
 *
 * NB: the result of the evaluation is no longer valid after this is done,
 * unless it is a pass-by-value datatype.
 *
 * NB: if you change this code, see also the hacks in exec_assign_value's
 * PLPGSQL_DTYPE_ARRAYELEM case for partial cleanup after subscript evals.
 * ----------
 */
static void
exec_eval_cleanup(PLpgSQL_execstate *estate)
{
    /* Clear result of a full SPI_execute */
    if (estate->eval_tuptable != NULL)
        SPI_freetuptable(estate->eval_tuptable);
    estate->eval_tuptable = NULL;
}

/* ----------
 * Generate a prepared plan
 * ----------
 */
static void
exec_prepare_plan(PLpgSQL_execstate *estate,
                  PLpgSQL_expr *expr, int cursorOptions,
                  bool keepplan)
{
    BEPIPlanIndex planIndex;
    /*
	 * 语法在构建解析树时不能方便地设置expr->func，所以请确保在解析器的hook需要它之前设置它
	 */
    expr->func = estate->func;

    /*
	 * 生成和保存plan
	 */
	PLSQLData *pData = PLSQL_GetCurrentThread_Data();
    if (pData) {
        pData->var_arg_env = (void *)expr;
    }
    planIndex = BEPI_prepare_params(estate->handler, expr->query, cursorOptions);
    // TODO: 考虑返回错误
    expr->planIndex = planIndex;
    /*
	 * Mark expression as not using a read-write param.  exec_assign_value has
	 * to take steps to override this if appropriate; that seems cleaner than
	 * adding parameters to all other callers.
	 */
    expr->rwparam = -1;
}

/* ----------
 * exec_stmt_execsql			Execute an SQL statement (possibly with INTO).
 *
 * Note: some callers rely on this not touching stmt_mcontext.  If it ever
 * needs to use that, fix those callers to push/pop stmt_mcontext.
 * ----------
 */
static int
exec_stmt_execsql(PLpgSQL_execstate *estate,
                  PLpgSQL_stmt_execsql *stmt)
{
    ParamListInfo paramLI;
    long tcount;
    int rc;
    PLpgSQL_expr *expr = stmt->sqlstmt;
    int too_many_rows_level = 0;
    bool is_query_or_hasReturning = false;

    if (plpgsql_extra_errors & PLPGSQL_XCHECK_TOOMANYROWS)
        too_many_rows_level = ERROR;
    else if (plpgsql_extra_warnings & PLPGSQL_XCHECK_TOOMANYROWS)
        too_many_rows_level = WARNING;

    // visit expr->ns to add unbuilt variable to datum list in GO side or add alias name for exist variable
    add_unvisited_variable_to_go(estate, stmt->sqlstmt);

    /*
	 * On the first call for this statement generate the plan, and detect
	 * whether the statement is INSERT/UPDATE/DELETE
	 */
    exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK, true);

    /*
	 * Set up ParamListInfo to pass to executor
	 */
    paramLI = setup_param_list(estate, expr);

    /*
	 * If we have INTO, then we only need one row back ... but if we have INTO
	 * STRICT or extra check too_many_rows, ask for two rows, so that we can
	 * verify the statement returns only one.  INSERT/UPDATE/DELETE are always
	 * treated strictly. Without INTO, just run the statement to completion
	 * (tcount = 0).
	 *
	 * We could just ask for two rows always when using INTO, but there are
	 * some cases where demanding the extra row costs significant time, eg by
	 * forcing completion of a sequential scan.  So don't do it unless we need
	 * to enforce strictness.
	 */
    if (stmt->into)
    {
        if (stmt->strict || stmt->mod_stmt || too_many_rows_level)
            tcount = 2;
        else
            tcount = 1;
    }
    else
        tcount = 0;

    /*
	 * Execute the plan
	 */
    rc = BEPI_execute_plan_with_paramlist(estate->handler, expr->planIndex, paramLI,
                                          estate->readonly_func, tcount, &is_query_or_hasReturning);

    /*
	 * Check for error, and set FOUND if appropriate (for historical reasons
	 * we set FOUND only for certain query types).  Also Assert that we
	 * identified the statement type the same as SPI did.
	 */
    switch (rc)
    {
    case SPI_OK_SELECT:
        // set found in GO
        BepiSetFound(estate->handler);
        exec_set_found(estate, true);
        break;

    case SPI_OK_INSERT:
    case SPI_OK_UPDATE:
    case SPI_OK_DELETE:
    case SPI_OK_INSERT_RETURNING:
    case SPI_OK_UPDATE_RETURNING:
    case SPI_OK_DELETE_RETURNING:
        exec_set_found(estate, (SPI_processed != 0));
        break;

    case SPI_OK_SELINTO:
    case SPI_OK_UTILITY:
        Assert(!stmt->mod_stmt);
        break;

    case SPI_OK_REWRITTEN:

        /*
		 * The command was rewritten into another kind of command. It's
		 * not clear what FOUND would mean in that case (and SPI doesn't
		 * return the row count either), so just set it to false.  Note
		 * that we can't assert anything about mod_stmt here.
		 */
        exec_set_found(estate, false);
        break;

        /* Some SPI errors deserve specific error messages */
    case SPI_ERROR_COPY:
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot COPY to/from client in PL/pgSQL")));
        break;


    case SPI_ERROR_TRANSACTION:
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("unsupported transaction command in PL/pgSQL")));
        break;

    default:
        elog(ERROR, "SPI_execute_plan_with_paramlist failed executing query \"%s\": %s",
             expr->query, SPI_result_code_string(rc));
        break;
    }

    if (stmt->into)
    {
        SPITupleTable *tuptab = (SPITupleTable*)palloc(sizeof(SPITupleTable));
        tuptab->vals = (HeapTuple*)palloc(sizeof(HeapTupleData));
        tuptab->tupdesc = (TupleDesc)palloc(sizeof(TupleDescData));
        tuptab->rocksDB_get = true;
        tuptab->tupdesc->rocksDB_get = true;
        uint64 n = SPI_processed;
        PLpgSQL_variable *target;

        /* Fetch target's datum entry */
        target = (PLpgSQL_variable *)estate->datums[stmt->target->dno];

        if (n > 1 && (stmt->strict || stmt->mod_stmt || too_many_rows_level))
        {
            char *errdetail;
            int errlevel;

            if (estate->func->print_strict_params)
                errdetail = format_expr_params(estate, expr);
            else
                errdetail = NULL;
            errlevel = (stmt->strict || stmt->mod_stmt) ? ERROR : too_many_rows_level;
            ereport(errlevel,
            (errcode(ERRCODE_TOO_MANY_ROWS),
            errmsg("query returned more than one row"),
            errdetail ? errdetail_internal("parameters: %s", errdetail) : 0,
            errhint("Make sure the query returns a single row, or use LIMIT 1.")));
        }
        /* Put the first result row into the target */
        exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc, 0);
        

        /* Clean up */
        exec_eval_cleanup(estate);
        SPI_freetuptable(SPI_tuptable);
	    tuptab->rocksDB_get = false;
	    tuptab->tupdesc->rocksDB_get = false;
    }
    else
    {
        /* If the statement returned a tuple table, complain */
        if (is_query_or_hasReturning)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("query has no destination for result data"),
                     (rc == SPI_OK_SELECT) ? errhint("If you want to discard the results of a SELECT, use PERFORM instead.") : 0));
    }

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_dynexecute			Execute a dynamic SQL query
 *					(possibly with INTO).
 * ----------
 */
static int
exec_stmt_dynexecute(PLpgSQL_execstate *estate,
                     PLpgSQL_stmt_dynexecute *stmt)
{
    Datum query;
    bool isnull;
    Oid restype;
    int32 restypmod;
    char *querystr;
    int exec_res;
    PreparedParamsData *ppd = NULL;
    bool is_query_or_hasReturning = false;
    MemoryContext stmt_mcontext = get_stmt_mcontext(estate);

    /*
	 * First we evaluate the string expression after the EXECUTE keyword. Its
	 * result is the querystring we have to execute.
	 */
    query = exec_eval_expr(estate, stmt->query, NULL, &isnull, &restype, &restypmod);
    if (isnull)
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("query string argument of EXECUTE is null")));

    /* Get the C-String representation */
    querystr = convert_value_to_string(estate, query, restype);
    //    querystr = DatumGetCString(query);

    /* copy it into the stmt_mcontext before we clean up */
    querystr = MemoryContextStrdup(stmt_mcontext, querystr);

    exec_eval_cleanup(estate);

    BEPIPlanIndex planIndex = BEPI_NOCACHED_PLAN;
    char *QueryStrWithParams;
    if (stmt->params) {
        // update query string
        ppd = exec_eval_using_params(estate, stmt->params);
        int i;
        QueryStrWithParams = (char *)palloc(1024);
        for (i = 0; i < ppd->nargs; i++) {
            char *srcSubStr = (char *)palloc(sizeof(char) * 4);
            sprintf(srcSubStr, "$%d", i+1);
            char *destSubStr = convert_value_to_string(estate, ppd->values[i], ppd->types[i]);
            QueryStrWithParams = ReplaceSubStr(querystr, srcSubStr, destSubStr, QueryStrWithParams);
        }
    }
    else {
        QueryStrWithParams = querystr;
    }

        planIndex = BEPI_prepare_params(estate->handler, QueryStrWithParams, 0);

    /*
	 * Execute the query without preparing a saved plan.
	 */
    if (stmt)
    {
        exec_res = BEPI_execute_plan_with_paramlist(estate->handler, planIndex, NULL, false, 0, &is_query_or_hasReturning);
    }
    else
        exec_res = SPI_execute(querystr, estate->readonly_func, 0);

    switch (exec_res)
    {
    case SPI_OK_SELECT:
    case SPI_OK_INSERT:
    case SPI_OK_UPDATE:
    case SPI_OK_DELETE:
    case SPI_OK_INSERT_RETURNING:
    case SPI_OK_UPDATE_RETURNING:
    case SPI_OK_DELETE_RETURNING:
    case SPI_OK_UTILITY:
    case SPI_OK_REWRITTEN:
        break;

    case 0:

        /*
		 * Also allow a zero return, which implies the querystring
		 * contained no commands.
		 */
        break;

    case SPI_OK_SELINTO:

        /*
		 * We want to disallow SELECT INTO for now, because its behavior
		 * is not consistent with SELECT INTO in a normal plpgsql context.
		 * (We need to reimplement EXECUTE to parse the string as a
		 * plpgsql command, not just feed it to SPI_execute.)  This is not
		 * a functional limitation because CREATE TABLE AS is allowed.
		 */
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("EXECUTE of SELECT ... INTO is not implemented"),
                 errhint("You might want to use EXECUTE ... INTO or EXECUTE CREATE TABLE ... AS instead.")));
        break;

        /* Some SPI errors deserve specific error messages */
    case SPI_ERROR_COPY:
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot COPY to/from client in PL/pgSQL")));
        break;

    case SPI_ERROR_TRANSACTION:
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("EXECUTE of transaction commands is not implemented")));
        break;

    default:
        elog(ERROR, "SPI_execute failed executing query \"%s\": %s",
             querystr, SPI_result_code_string(exec_res));
        break;
    }

    /* Save result info for GET DIAGNOSTICS */
    estate->eval_processed = SPI_processed;

    /* Process INTO if present */
    if (stmt->into)
    {
        SPITupleTable *tuptable =
                (SPITupleTable *)palloc0(sizeof(SPITupleTable));
        tuptable->tuptabcxt = estate->stmt_mcontext;
        tuptable->subid = 100;
        /* set up initial allocations */
        tuptable->alloced = 128;
        tuptable->vals = (HeapTuple *)palloc(tuptable->alloced * sizeof(HeapTuple));
        tuptable->vals[0] = (HeapTuple)palloc((sizeof(HeapTupleData)));
        tuptable->vals[0]->values = (Datum *)palloc(sizeof(Datum));

        tuptable->numvals = 0;

        TupleDesc tupleDesc = (TupleDesc)palloc(sizeof(TupleDescData));
        tupleDesc->rocksDB_get = false;
        tupleDesc->natts = 1;
        tupleDesc->tdrefcount = 0;
        tupleDesc->tdtypeid = 0;
        tupleDesc->tdtypmod = 0;
        TupleConstr tempConstr;
        tupleDesc->constr = &tempConstr;
        tuptable->tupdesc = tupleDesc;
        tuptable->tupdesc->natts = 1;

        SPITupleTable *tuptab = tuptable;
        uint64 n = 1;

        PLpgSQL_variable *target;

        /* If the statement did not return a tuple table, complain */
        if (tuptab == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("INTO used with a command that cannot return data")));

        /* Fetch target's datum entry */
        target = (PLpgSQL_variable *)estate->datums[stmt->target->dno];

        /*
		 * If SELECT ... INTO specified STRICT, and the query didn't find
		 * exactly one row, throw an error.  If STRICT was not specified, then
		 * allow the query to find any number of rows.
		 */
        if (n == 0)
        {
            if (stmt->strict)
            {
                char *errdetail;

                if (estate->func->print_strict_params)
                    errdetail = format_preparedparamsdata(estate, ppd);
                else
                    errdetail = NULL;
            }
            /* set the target to NULL(s) */
            exec_move_row(estate, target, NULL, tuptab->tupdesc, 0);
        }
        else
        {
            if (n > 1 && stmt->strict)
            {
                char *errdetail;

                if (estate->func->print_strict_params)
                    errdetail = format_preparedparamsdata(estate, ppd);
                else
                    errdetail = NULL;
            }

            /* Put the first result row into the target */
            exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc, 0);
        }
        /* clean up after exec_move_row() */
        exec_eval_cleanup(estate);
    }

    /* Release any result from SPI_execute, as well as transient data */
    SPI_freetuptable(SPI_tuptable);
    MemoryContextReset(stmt_mcontext);

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_dynfors			Execute a dynamic query, assign each
 *					tuple to a record or row and
 *					execute a group of statements
 *					for it.
 * ----------
 */
static int
exec_stmt_dynfors(PLpgSQL_execstate *estate, PLpgSQL_stmt_dynfors *stmt)
{
    Portal portal;
    int rc;

    portal = exec_dynquery_with_params(estate, stmt->query, stmt->params,
                                       NULL, 0);

    /*
	 * Execute the loop
	 */
    rc = exec_for_query(estate, (PLpgSQL_stmt_forq *)stmt, portal, true);

    /*
	 * Close the implicit cursor
	 */
    SPI_cursor_close(portal);

    return rc;
}

/* ----------
 * exec_stmt_open			Execute an OPEN cursor statement
 * ----------
 */
static int
exec_stmt_open(PLpgSQL_execstate *estate, PLpgSQL_stmt_open *stmt)
{
    PLpgSQL_var *curvar;
    MemoryContext stmt_mcontext = NULL;
    char *curname = NULL;
    PLpgSQL_expr *query = NULL;
    Portal portal;
    ParamListInfo paramLI;
    char *cur_argstr = NULL;

    /* ----------
	 * Get the cursor variable and if it has an assigned name, check
	 * that it's not in use currently.
	 * ----------
	 */
    curvar = (PLpgSQL_var *)(estate->datums[stmt->curvar]);
    add_unvisited_variable_to_go(estate, curvar->cursor_explicit_expr);

    if (true)
    {
        MemoryContext oldcontext;

        /* We only need stmt_mcontext to hold the cursor name string */
        stmt_mcontext = get_stmt_mcontext(estate);
        oldcontext = MemoryContextSwitchTo(stmt_mcontext);
        MemoryContextSwitchTo(oldcontext);
        char *errmg = (char *)palloc(ERRSIZE);
        memset(errmg, 0, ERRSIZE);
        CursorDesc cursorDesc = BEPI_cursor_find(estate->handler, curvar->refname, &errmg);
	    if (strlen(errmg) > 0) {
            ereport(ERROR, (errmsg("%s", errmg)));
        }
        pfree(errmg);
        int nameLen = strlen(cursorDesc.name);
        curname = (char *)palloc(nameLen+1);
        memset(curname, 0, nameLen + 1);
        memcpy(curname, cursorDesc.name, nameLen);
        curname[nameLen] = '\0';
        if (cursorDesc.name == NULL || strlen(cursorDesc.name) == 0)
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_CURSOR),
                     errmsg("cursor \"%s\" already in use", curname)));
    }

    /* ----------
	 * Process the OPEN according to it's type.
	 * ----------
	 */
    if (stmt->query != NULL)
    {
        /* ----------
		 * This is an OPEN refcursor FOR SELECT ...
		 *
		 * We just make sure the query is planned. The real work is
		 * done downstairs.
		 * ----------
		 */
        query = stmt->query;
        if (query->plan == NULL)
            exec_prepare_plan(estate, query, stmt->cursor_options, true);
    }
    else if (stmt->dynquery != NULL)
    {
        /* ----------
		 * This is an OPEN refcursor FOR EXECUTE ...
		 * ----------
		 */
        portal = exec_dynquery_with_params(estate,
                                           stmt->dynquery,
                                           stmt->params,
                                           curname,
                                           stmt->cursor_options);

        /*
		 * If cursor variable was NULL, store the generated portal name in it.
		 * Note: exec_dynquery_with_params already reset the stmt_mcontext, so
		 * curname is a dangling pointer here; but testing it for nullness is
		 * OK.
		 */
        if (curname == NULL)
            assign_text_var(estate, curvar, portal->name);

        return PLPGSQL_RC_OK;
    }
    else
    {
        /* ----------
		 * This is an OPEN cursor
		 *
		 * Note: parser should already have checked that statement supplies
		 * args iff cursor needs them, but we check again to be safe.
		 * ----------
		 */
        if (stmt->argquery != NULL)
        {
            cur_argstr = palloc(strlen(stmt->argquery->query)-7);
            int i, j;
            for (i = 7, j = 0; i < strlen(stmt->argquery->query)-1; i++) {
                cur_argstr[j++] = stmt->argquery->query[i];
            }
            cur_argstr[j] = '\0';

            /* ----------
			 * OPEN CURSOR with args.  We fake a SELECT ... INTO ...
			 * statement to evaluate the args and put 'em into the
			 * internal row.
			 * ----------
			 */
            PLpgSQL_stmt_execsql set_args;

            if (curvar->cursor_explicit_argrow < 0)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("arguments given for cursor without arguments")));

            memset(&set_args, 0, sizeof(set_args));
            set_args.cmd_type = PLPGSQL_STMT_EXECSQL;
            set_args.lineno = stmt->lineno;
            set_args.sqlstmt = stmt->argquery;
            set_args.into = true;
            /* XXX historically this has not been STRICT */
            set_args.target = (PLpgSQL_variable *)(estate->datums[curvar->cursor_explicit_argrow]);
        }
        else
        {
            if (curvar->cursor_explicit_argrow >= 0)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("arguments required for cursor")));
        }

        query = curvar->cursor_explicit_expr;
    }

    /*
	 * Set up ParamListInfo for this query
	 */
    paramLI = setup_param_list(estate, query);

    /*
	 * Open the cursor (the paramlist will get copied into the portal)
	 */
    int openState = BEPI_cursor_open_with_paramlist(estate->handler, curname, paramLI, cur_argstr, false);
	pfree(curname);
	pfree(cur_argstr);
    if (openState == 0) {
    	estate->cur_state[stmt->curvar] = CURSOR_OPENED;
        return PLPGSQL_RC_OK;
    }
    ereport(ERROR, (errmsg("error happen when open cursor: \"%s\"",
                          "open cursor failed!")));
    return PLPGSQL_RC_EXIT;
}

/* ----------
 * exec_stmt_fetch			Fetch from a cursor into a target, or just
 *							move the current position of the cursor
 * ----------
 */
static int
exec_stmt_fetch(PLpgSQL_execstate *estate, PLpgSQL_stmt_fetch *stmt)
{
    PLpgSQL_var *curvar;
    long how_many = stmt->how_many;
    SPITupleTable *tuptab;
    // Portal portal;
    char *curname;
    uint64 n;
    // MemoryContext oldcontext;
    add_unvisited_variable_to_go(estate, stmt->expr);
    /* ----------
	 * Get the portal of the cursor by name
	 * ----------
	 */
    curvar = (PLpgSQL_var *)(estate->datums[stmt->curvar]);

    curvar->isnull = false;
    if (curvar->isnull)
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("cursor variable \"%s\" is null", curvar->refname)));
    /* Use eval_mcontext for short-lived string */
    curname = curvar->refname;
    char *errmg = (char *)palloc(ERRSIZE);
	memset(errmg, 0, ERRSIZE);
    CursorDesc cursorDesc = BEPI_cursor_find(estate->handler, curvar->refname, &errmg);
    if (strlen(errmg) > 0) {
        ereport(ERROR, (errmsg("%s", errmg)));
        pfree(errmg);
    }
    pfree(errmg);
    /* Calculate position for FETCH_RELATIVE or FETCH_ABSOLUTE */
    if (stmt->expr)
    {
        bool isnull;
        /* XXX should be doing this in LONG not INT width */
        how_many = exec_eval_integer(estate, stmt->expr, &isnull);
        if (isnull)
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("relative or absolute cursor position is null")));
        exec_eval_cleanup(estate);
    }
    if (!stmt->is_move)
    {
        PLpgSQL_variable *target;
        target = (PLpgSQL_variable *)estate->datums[stmt->target->dno];
        bool found = false;
        found = BEPI_cursor_fetch(estate->handler, curname, stmt->direction, how_many);
        n = found;
        SPITupleTable *tuptable =
        (SPITupleTable *)palloc0(sizeof(SPITupleTable));
        tuptable->tuptabcxt = estate->stmt_mcontext;
        tuptable->subid = 100;
        /* set up initial allocations */
        tuptable->alloced = 128;
        tuptable->vals = (HeapTuple *)palloc(tuptable->alloced * sizeof(HeapTuple));
        tuptable->vals[0] = (HeapTuple)palloc((sizeof(HeapTupleData)));
        tuptable->vals[0]->values = (Datum *)palloc(sizeof(Datum));
        tuptable->numvals = 0;
        TupleDesc tupleDesc = (TupleDesc)palloc(sizeof(TupleDescData));
        tupleDesc->rocksDB_get = false;
        tupleDesc->natts = 1;
        tupleDesc->tdrefcount = 0;
        tupleDesc->tdtypeid = 0;
        tupleDesc->tdtypmod = 0;
        TupleConstr tempConstr;
        tupleDesc->constr = &tempConstr;
        tuptable->tupdesc = tupleDesc;
        tuptable->tupdesc->natts = 1;
        tuptab = tuptable;
        if (n == 0)
            exec_move_row(estate, target, NULL, tuptab->tupdesc, true);
        else
            exec_move_row(estate, target, tuptab->vals[0], tuptab->tupdesc, true);
        BepiResetCursorAssign();
        exec_eval_cleanup(estate);
    }
    else
    {
        /* Move the cursor */
        BEPI_cursor_move(estate->handler, cursorDesc.name, stmt->direction, how_many);
        n = 1;
    }
    /* Set the ROW_COUNT and the global FOUND variable appropriately. */
    estate->eval_processed = n;
    exec_set_found(estate, n != 0);
    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_stmt_close			Close a cursor
 * ----------
 */
static int
exec_stmt_close(PLpgSQL_execstate *estate, PLpgSQL_stmt_close *stmt)
{
    PLpgSQL_var *curvar;
    /* ----------
	 * Get the portal of the cursor by name
	 * ----------
	 */
    curvar = (PLpgSQL_var *)(estate->datums[stmt->curvar]);
    if (curvar->isnull && curvar->refname==NULL)
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("cursor variable \"%s\" is null", curvar->refname)));
    char *errmg = (char *)palloc(ERRSIZE);
	memset(errmg, 0, ERRSIZE);
    CursorDesc cursorDesc = BEPI_cursor_find(estate->handler, curvar->refname, &errmg);
	if (strlen(errmg) > 0) {
        ereport(ERROR, (errmsg("%s", errmg)));
    }
    pfree(errmg);
    char *err = (char *)palloc(ERRSIZE);
    memset(err, 0, ERRSIZE);
    BEPI_cursor_close(estate->handler, cursorDesc. name, err);
    if (err != NULL && strlen(err) != 0) {
        ereport(ERROR,
                (errmsg("error hanpen when close cursor: \"%%s\""), err));
    }
    estate->cur_state[stmt->curvar] = CURSOR_CLOSED;
    return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_commit
 *
 * Commit the transaction.
 */
static int
exec_stmt_commit(PLpgSQL_execstate *estate, PLpgSQL_stmt_commit *stmt)
{
    ereport(ERROR,
            (errmsg("don't support commit")));
    BEPI_TXN_commit(estate->handler);
    return PLPGSQL_RC_OK;
}

/*
 * exec_stmt_set
 *
 * Execute SET/RESET statement.
 *
 * We just parse and execute the statement normally, but we have to do it
 * without setting a snapshot, for things like SET TRANSACTION.
 */
static int
exec_stmt_set(PLpgSQL_execstate *estate, PLpgSQL_stmt_set *stmt)
{
    PLpgSQL_expr *expr = stmt->expr;
    int rc;
    add_unvisited_variable_to_go(estate, stmt->expr);

    exec_prepare_plan(estate, expr, 0, true);
    bool is_query_or_hasReturning = false;
    BepiExecPlan(estate->handler,expr->planIndex, &is_query_or_hasReturning);

    return PLPGSQL_RC_OK;
    rc = SPI_execute_plan(expr->plan, NULL, NULL, estate->readonly_func, 0);

    if (rc != SPI_OK_UTILITY)
        elog(ERROR, "SPI_execute_plan failed executing query \"%s\": %s",
             expr->query, SPI_result_code_string(rc));

    return PLPGSQL_RC_OK;
}

/* ----------
 * exec_assign_expr			Put an expression's result into a variable.
 * ----------
 */
static void
exec_assign_expr(PLpgSQL_execstate *estate, PLpgSQL_datum* target,
                 PLpgSQL_expr *expr,int targetno)
{
    Datum value;
    bool isnull;
    Oid valtype;
    int32 valtypmod;
    /*
	 * If first time through, create a plan for this expression, and then see
	 * if we can pass the target variable as a read-write parameter to the
	 * expression.  (This is a bit messy, but it seems cleaner than modifying
	 * the API of exec_eval_expr for the purpose.)
	 */
    PLpgSQL_var *var = (PLpgSQL_var *)target;
    if (var->datatype->typoid == REFCURSOROID) {
        // get cursor arg name and type here
        cursorArg_name_type *curArg = NULL;
        char **arg_names = NULL;
        char **arg_type_names = NULL;
        int arg_num = 0;
        if (var->cursor_explicit_argrow >= 0) {
            PLpgSQL_row *row = (PLpgSQL_row *)(estate->datums[var->cursor_explicit_argrow]);
            // int anum = 0;
            int fnum;
            if (row->nfields > 0) {
                arg_num = row->nfields;
                curArg = (cursorArg_name_type *)palloc(sizeof(cursorArg_name_type) * row->nfields);
                arg_names = (char**)palloc(sizeof(char *) * row->nfields);
                arg_type_names = (char**)palloc(sizeof(char *) * row->nfields);
            }
            for (fnum = 0; fnum < row->nfields; fnum++) {
                PLpgSQL_var *tempVar;
                // Datum		value;
                // bool		isnull;
                // Oid			valtype;
                // int32		valtypmod;

                tempVar = (PLpgSQL_var *) (estate->datums[row->varnos[fnum]]);
                *(arg_names + fnum) = (char *)palloc(strlen(tempVar->refname));
                *(arg_type_names + fnum) = (char *)palloc(strlen(tempVar->datatype->typname));
                strcpy(*(arg_names + fnum), tempVar->refname);
                strcpy(*(arg_type_names + fnum), tempVar->datatype->typname);

                (curArg + fnum)->name = (char *)palloc(strlen(tempVar->refname));
                (curArg + fnum)->type_name = (char *)palloc(strlen(tempVar->datatype->typname));
                strcpy((*(curArg + fnum)).name, tempVar->refname);
                (*(curArg + fnum)).typoid = tempVar->datatype->typoid;
                // get type name from oid
                char *err = NULL;
                char *arg_type = GetTypeNameFromOid(tempVar->datatype->typoid, &err);
                if (err != NULL) {
                    ereport(ERROR,
                                (errmsg("%s", err)));
                }
                strcpy((*(curArg + fnum)).type_name, arg_type);
                pfree(err);
            }
        }
		int state = BEPI_cursor_declare(estate->handler, ((PLpgSQL_var *)(target))->refname, ((PLpgSQL_var *)(target))->default_val->query, curArg, arg_num);
		if (state == -1) {
			ereport(ERROR,
					(errmsg("error hapen when declare cursor %s", ((PLpgSQL_var *)(target))->refname)));
		}
		return ;
	}

    if (expr->planIndex == BEPI_NOCACHED_PLAN)
    {
        exec_prepare_plan(estate, expr, 0, true);
        if (target->dtype == PLPGSQL_DTYPE_VAR)
            exec_check_rw_parameter(expr, target->dno);
    }
    value = exec_eval_expr(estate, expr, target, &isnull, &valtype, &valtypmod);
    exec_assign_value(estate, target, value, isnull, valtype, valtypmod, 0,target->dno,0, false, -1);
    exec_eval_cleanup(estate);
}

/* ----------
 * exec_assign_c_string		Put a C string into a text variable.
 *
 * We take a NULL pointer as signifying empty string, not SQL null.
 *
 * As with the underlying exec_assign_value, caller is expected to do
 * exec_eval_cleanup later.
 * ----------
 */
static void
exec_assign_c_string(PLpgSQL_execstate *estate, PLpgSQL_datum *target,
                     const char *str)
{
    text *value = NULL;
    MemoryContext oldcontext;

    /* Use eval_mcontext for short-lived text value */
    oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
    MemoryContextSwitchTo(oldcontext);

    exec_assign_value(estate, target, PointerGetDatum(value), false,
                      TEXTOID, -1, 0,target->dno,0, false, -1);
}

/* ----------
 * exec_assign_value			Put a value into a target datum
 *
 * Note: in some code paths, this will leak memory in the eval_mcontext;
 * we assume that will be cleaned up later by exec_eval_cleanup.  We cannot
 * call exec_eval_cleanup here for fear of destroying the input Datum value.
 * ----------
 * 
 */
static void
exec_assign_value(PLpgSQL_execstate *estate,
                  PLpgSQL_datum *target,
                  Datum value, bool isNull,
                  Oid valtype, int32 valtypmod,
                  bool inFetch,
                  int targetno,
                  int valueno,
                  bool inForEach,
                  int index)
{
    switch (target->dtype)
    {
    case PLPGSQL_DTYPE_VAR:
    case PLPGSQL_DTYPE_PROMISE:
    {
        /*
		 * Target is a variable
		 */
        PLpgSQL_var *var = (PLpgSQL_var *)target;
        Datum newvalue = value;

        if (isNull && var->notnull)
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("null value cannot be assigned to variable \"%s\" declared NOT NULL",
                            var->refname)));

        /*
		 * If type is by-reference, copy the new value (which is
		 * probably in the eval_mcontext) into the procedure's main
		 * memory context.  But if it's a read/write reference to an
		 * expanded object, no physical copy needs to happen; at most
		 * we need to reparent the object's memory context.
		 *
		 * If it's an array, we force the value to be stored in R/W
		 * expanded form.  This wins if the function later does, say,
		 * a lot of array subscripting operations on the variable, and
		 * otherwise might lose.  We might need to use a different
		 * heuristic, but it's too soon to tell.  Also, are there
		 * cases where it'd be useful to force non-array values into
		 * expanded form?
		 */

        /*
		 * Now free the old value, if any, and assign the new one. But
		 * skip the assignment if old and new values are the same.
		 * Note that for expanded objects, this test is necessary and
		 * cannot reliably be made any earlier; we have to be looking
		 * at the object's standard R/W pointer to be sure pointer
		 * equality is meaningful.
		 *
		 * Also, if it's a promise variable, we should disarm the
		 * promise in any case --- otherwise, assigning null to an
		 * armed promise variable would fail to disarm the promise.
		 */
        char * errRet ;
        if (var->value != newvalue || var->isnull || isNull)
            errRet = AssignTheValue(estate->handler,  targetno, valueno, inFetch, inForEach, index);
            if (strlen(errRet)!=0){
                ereport(ERROR, (errmsg("%s", errRet)));
            }
        else
            var->promise = PLPGSQL_PROMISE_NONE;
        break;
    }

    case PLPGSQL_DTYPE_ROW:
    {
        /*
		 * Target is a row variable
		 */
        PLpgSQL_row *row = (PLpgSQL_row *)target;

        if (isNull)
        {
            /* If source is null, just assign nulls to the row */
            exec_move_row(estate, (PLpgSQL_variable *) row,
                          NULL, NULL, inFetch);
        }
        else
        {
            /* Source must be of RECORD or composite type */
            /*if (!type_is_rowtype(valtype))*/
            /*ereport(ERROR,*/
            /*(errcode(ERRCODE_DATATYPE_MISMATCH),*/
            /*errmsg("cannot assign non-composite value to a row variable")));*/
            exec_move_row_from_datum(estate, (PLpgSQL_variable *)row,
                                     value, inFetch);
        }
        break;
    }

    case PLPGSQL_DTYPE_REC:
    {
        /*
				 * Target is a record variable
				 */
        PLpgSQL_rec *rec = (PLpgSQL_rec *)target;

        if (isNull)
        {
            if (rec->notnull)
                ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                         errmsg("null value cannot be assigned to variable \"%s\" declared NOT NULL",
                                rec->refname)));

            /* Set variable to a simple NULL */
            exec_move_row(estate, (PLpgSQL_variable *) rec,
                          NULL, NULL, 0);
        }
        else
        {
            char *errRet = AssignTheValue(estate->handler,  targetno, -1, inFetch, inForEach, index);
            if (strlen(errRet)!=0){
                ereport(ERROR, (errmsg("%s", errRet)));
            }
        }
        break;
    }

    case PLPGSQL_DTYPE_RECFIELD:
    {
        /*
		 * Target is a field of a record
		 */
        PLpgSQL_recfield *recfield = (PLpgSQL_recfield *)target;
        PLpgSQL_rec *rec;
        ExpandedRecordHeader *erh;

        rec = (PLpgSQL_rec *)(estate->datums[recfield->recparentno]);
        erh = rec->erh;

        /*
		 * If record variable is NULL, instantiate it if it has a
		 * named composite type, else complain.  (This won't change
		 * the logical state of the record, but if we successfully
		 * assign below, the unassigned fields will all become NULLs.)
		 */
        if (erh == NULL)
        {
            /*instantiate_empty_record_variable(estate, rec);*/
            erh = rec->erh;
        }

        /*
		 * Look up the field's properties if we have not already, or
		 * if the tuple descriptor ID changed since last time.
		 */
        if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
        {
            recfield->rectupledescid = erh->er_tupdesc_id;
        }

        /* We don't support assignments to system columns. */
        if (recfield->finfo.fnumber <= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot assign to system column \"%s\"",
                            recfield->fieldname)));
        break;
    }

    case PLPGSQL_DTYPE_ARRAYELEM:
    {
        /*
		 * Target is an element of an array
		 */
        PLpgSQL_arrayelem *arrayelem;
        int nsubscripts;
        int i;
        PLpgSQL_expr *subscripts[MAXDIM];
        int subscriptvals[MAXDIM];
        Datum oldarraydatum,
            newarraydatum,
            coerced_value ;
        newarraydatum = (Datum)0;
        bool oldarrayisnull;
        Oid parenttypoid;
        int32 parenttypmod;
        SPITupleTable *save_eval_tuptable;
        MemoryContext oldcontext;

        /*
		 * We need to do subscript evaluation, which might require
		 * evaluating general expressions; and the caller might have
		 * done that too in order to prepare the input Datum.  We have
		 * to save and restore the caller's SPI_execute result, if
		 * any.
		 */
        save_eval_tuptable = estate->eval_tuptable;
        estate->eval_tuptable = NULL;

        /*
		 * To handle constructs like x[1][2] := something, we have to
		 * be prepared to deal with a chain of arrayelem datums. Chase
		 * back to find the base array datum, and save the subscript
		 * expressions as we go.  (We are scanning right to left here,
		 * but want to evaluate the subscripts left-to-right to
		 * minimize surprises.)  Note that arrayelem is left pointing
		 * to the leftmost arrayelem datum, where we will cache the
		 * array element type data.
		 */
        nsubscripts = 0;
        do
        {
            arrayelem = (PLpgSQL_arrayelem *)target;
            if (nsubscripts >= MAXDIM)
                ereport(ERROR,
                        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                         errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                                nsubscripts + 1, MAXDIM)));
            subscripts[nsubscripts++] = arrayelem->subscript;
            target = estate->datums[arrayelem->arrayparentno];
        } while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);

        /* Fetch current value of array datum */
        exec_eval_datum(estate, target,
                        &parenttypoid, &parenttypmod,
                        &oldarraydatum, &oldarrayisnull);

        /* Update cached type data if necessary */
        if (arrayelem->parenttypoid != parenttypoid ||
            arrayelem->parenttypmod != parenttypmod)
        {
            Oid arraytypoid = (Datum)0;
            int32 arraytypmod = parenttypmod;
            int16 arraytyplen = 0;
            Oid elemtypoid = (Datum)0;
            int16 elemtyplen = 0;
            bool elemtypbyval = false; 
            char elemtypalign = '\0';

            if (!OidIsValid(elemtypoid))
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("subscripted object is not an array")));

            /* Now safe to update the cached data */
            arrayelem->parenttypoid = parenttypoid;
            arrayelem->parenttypmod = parenttypmod;
            arrayelem->arraytypoid = arraytypoid;
            arrayelem->arraytypmod = arraytypmod;
            arrayelem->arraytyplen = arraytyplen;
            arrayelem->elemtypoid = elemtypoid;
            arrayelem->elemtyplen = elemtyplen;
            arrayelem->elemtypbyval = elemtypbyval;
            arrayelem->elemtypalign = elemtypalign;
        }

        /*
		 * Evaluate the subscripts, switch into left-to-right order.
		 * Like the expression built by ExecInitSubscriptingRef(),
		 * complain if any subscript is null.
		 */
        for (i = 0; i < nsubscripts; i++)
        {
            bool subisnull;

            subscriptvals[i] =
                exec_eval_integer(estate,
                                  subscripts[nsubscripts - 1 - i],
                                  &subisnull);
            if (subisnull)
                ereport(ERROR,
                        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                         errmsg("array subscript in assignment must not be null")));

            /*
			 * Clean up in case the subscript expression wasn't
			 * simple. We can't do exec_eval_cleanup, but we can do
			 * this much (which is safe because the integer subscript
			 * value is surely pass-by-value), and we must do it in
			 * case the next subscript expression isn't simple either.
			 */
            if (estate->eval_tuptable != NULL)
                SPI_freetuptable(estate->eval_tuptable);
            estate->eval_tuptable = NULL;
        }

        /* Now we can restore caller's SPI_execute result if any. */
        Assert(estate->eval_tuptable == NULL);
        estate->eval_tuptable = save_eval_tuptable;

        /* Coerce source value to match array element type. */
        coerced_value = exec_cast_value(estate,
                                        value,
                                        &isNull,
                                        valtype,
                                        valtypmod,
                                        arrayelem->elemtypoid,
                                        arrayelem->arraytypmod);

        /*
		 * If the original array is null, cons up an empty array so
		 * that the assignment can proceed; we'll end with a
		 * one-element array containing just the assigned-to
		 * subscript.  This only works for varlena arrays, though; for
		 * fixed-length array types we skip the assignment.  We can't
		 * support assignment of a null entry into a fixed-length
		 * array, either, so that's a no-op too.  This is all ugly but
		 * corresponds to the current behavior of execExpr*.c.
		 */
        if (arrayelem->arraytyplen > 0 && /* fixed-length array? */
            (oldarrayisnull || isNull))
            return;

        /* empty array, if any, and newarraydatum are short-lived */
        oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
        MemoryContextSwitchTo(oldcontext);

        /*
		 * Assign the new array to the base variable.  It's never NULL
		 * at this point.  Note that if the target is a domain,
		 * coercing the base array type back up to the domain will
		 * happen within exec_assign_value.
		 */
        exec_assign_value(estate, target,
                          newarraydatum,
                          false,
                          arrayelem->arraytypoid,
                          arrayelem->arraytypmod, 0,arrayelem->arrayparentno,0, false, -1);
        break;
    }

    default:
        elog(ERROR, "unrecognized dtype: %d", target->dtype);
    }
}

/*
 * exec_eval_datum				Get current value of a PLpgSQL_datum
 *
 * The type oid, typmod, value in Datum format, and null flag are returned.
 *
 * At present this doesn't handle PLpgSQL_expr or PLpgSQL_arrayelem datums;
 * that's not needed because we never pass references to such datums to SPI.
 *
 * NOTE: the returned Datum points right at the stored value in the case of
 * pass-by-reference datatypes.  Generally callers should take care not to
 * modify the stored value.  Some callers intentionally manipulate variables
 * referenced by R/W expanded pointers, though; it is those callers'
 * responsibility that the results are semantically OK.
 *
 * In some cases we have to palloc a return value, and in such cases we put
 * it into the estate's eval_mcontext.
 */
static void
exec_eval_datum(PLpgSQL_execstate *estate,
                PLpgSQL_datum *datum,
                Oid *typeid,
                int32 *typetypmod,
                Datum *value,
                bool *isnull)
{
    MemoryContext oldcontext;

    switch (datum->dtype)
    {
    case PLPGSQL_DTYPE_PROMISE:

        /* FALL THRU */

    case PLPGSQL_DTYPE_VAR:
    {
        PLpgSQL_var *var = (PLpgSQL_var *)datum;

        *typeid = var->datatype->typoid;
        *typetypmod = var->datatype->atttypmod;
        *value = var->value;
        *isnull = var->isnull;
        break;
    }

    case PLPGSQL_DTYPE_ROW:
    {
        PLpgSQL_row *row = (PLpgSQL_row *)datum;
        HeapTuple tup;

        /* We get here if there are multiple OUT parameters */
        if (!row->rowtupdesc) /* should not happen */
            elog(ERROR, "row variable has no tupdesc");
        /* Make sure we have a valid type/typmod setting */
        /*BlessTupleDesc(row->rowtupdesc);*/
        oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
        tup = make_tuple_from_row(estate, row, row->rowtupdesc);
        if (tup == NULL) /* should not happen */
            elog(ERROR, "row not compatible with its own tupdesc");
        *typeid = row->rowtupdesc->tdtypeid;
        *typetypmod = row->rowtupdesc->tdtypmod;
        *isnull = false;
        MemoryContextSwitchTo(oldcontext);
        break;
    }

    case PLPGSQL_DTYPE_REC:
    {
        PLpgSQL_rec *rec = (PLpgSQL_rec *)datum;

        if (rec->erh == NULL)
        {
            /* Treat uninstantiated record as a simple NULL */
            *value = (Datum)0;
            *isnull = true;
            /* Report variable's declared type */
            *typeid = rec->rectypeid;
            *typetypmod = -1;
        }
        else
        {

            /* Empty record is also a NULL */
            *value = (Datum)0;
            *isnull = true;

            if (rec->rectypeid != RECORDOID)
            {
                /* Report variable's declared type, if not RECORD */
                *typeid = rec->rectypeid;
                *typetypmod = -1;
            }
            else
            {
                /* Report record's actual type if declared RECORD */
                *typeid = rec->erh->er_typeid;
                *typetypmod = rec->erh->er_typmod;
            }
        }
        break;
    }

    case PLPGSQL_DTYPE_RECFIELD:
    {
        PLpgSQL_recfield *recfield = (PLpgSQL_recfield *)datum;
        PLpgSQL_rec *rec;
        ExpandedRecordHeader *erh;

        rec = (PLpgSQL_rec *)(estate->datums[recfield->recparentno]);
        erh = rec->erh;

        /*
		 * If record variable is NULL, instantiate it if it has a
		 * named composite type, else complain.  (This won't change
		 * the logical state of the record: it's still NULL.)
		 */
        if (erh == NULL)
        {
            instantiate_empty_record_variable(estate, rec);
            erh = rec->erh;
        }

        /*
		 * Look up the field's properties if we have not already, or
		 * if the tuple descriptor ID changed since last time.
		 */
        if (unlikely(recfield->rectupledescid != erh->er_tupdesc_id))
        {
            recfield->rectupledescid = erh->er_tupdesc_id;
        }

        /* Report type data. */
        *typeid = recfield->finfo.ftypeid;
        *typetypmod = recfield->finfo.ftypmod;
        break;
    }

    default:
        elog(ERROR, "unrecognized dtype: %d", datum->dtype);
    }
}

/*
 * plpgsql_exec_get_datum_type				Get datatype of a PLpgSQL_datum
 *
 * This is the same logic as in exec_eval_datum, but we skip acquiring
 * the actual value of the variable.  Also, needn't support DTYPE_ROW.
 */
Oid plpgsql_exec_get_datum_type(PLpgSQL_execstate *estate,
                                PLpgSQL_datum *datum)
{
    Oid typeid;

    switch (datum->dtype)
    {
    case PLPGSQL_DTYPE_VAR:
    case PLPGSQL_DTYPE_PROMISE:
    {
        PLpgSQL_var *var = (PLpgSQL_var *)datum;

        typeid = var->datatype->typoid;
        break;
    }

    case PLPGSQL_DTYPE_REC:
    {
        PLpgSQL_rec *rec = (PLpgSQL_rec *)datum;

        if (rec->erh == NULL || rec->rectypeid != RECORDOID)
        {
            /* Report variable's declared type */
            typeid = rec->rectypeid;
        }
        else
        {
            /* Report record's actual type if declared RECORD */
            typeid = rec->erh->er_typeid;
        }
        break;
    }

    case PLPGSQL_DTYPE_RECFIELD:
    {
        PLpgSQL_recfield *recfield = (PLpgSQL_recfield *)datum;
        PLpgSQL_rec *rec;

        rec = (PLpgSQL_rec *)(estate->datums[recfield->recparentno]);

        /*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
        if (rec->erh == NULL)
            instantiate_empty_record_variable(estate, rec);

        /*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
        if (unlikely(recfield->rectupledescid != rec->erh->er_tupdesc_id))
        {
            recfield->rectupledescid = rec->erh->er_tupdesc_id;
        }

        typeid = recfield->finfo.ftypeid;
        break;
    }

    default:
        elog(ERROR, "unrecognized dtype: %d", datum->dtype);
        typeid = InvalidOid; /* keep compiler quiet */
        break;
    }

    return typeid;
}

/*
 * plpgsql_exec_get_datum_type_info			Get datatype etc of a PLpgSQL_datum
 *
 * An extended version of plpgsql_exec_get_datum_type, which also retrieves the
 * typmod and collation of the datum.  Note however that we don't report the
 * possibly-mutable typmod of RECORD values, but say -1 always.
 */
void plpgsql_exec_get_datum_type_info(PLpgSQL_execstate *estate,
                                      PLpgSQL_datum *datum,
                                      Oid *typeId, int32 *typMod, Oid *collation)
{
    switch (datum->dtype)
    {
    case PLPGSQL_DTYPE_VAR:
    case PLPGSQL_DTYPE_PROMISE:
    {
        PLpgSQL_var *var = (PLpgSQL_var *)datum;

        *typeId = var->datatype->typoid;
        *typMod = var->datatype->atttypmod;
        *collation = var->datatype->collation;
        break;
    }

    case PLPGSQL_DTYPE_REC:
    {
        PLpgSQL_rec *rec = (PLpgSQL_rec *)datum;

        if (rec->erh == NULL || rec->rectypeid != RECORDOID)
        {
            /* Report variable's declared type */
            *typeId = rec->rectypeid;
            *typMod = -1;
        }
        else
        {
            /* Report record's actual type if declared RECORD */
            *typeId = rec->erh->er_typeid;
            /* do NOT return the mutable typmod of a RECORD variable */
            *typMod = -1;
        }
        /* composite types are never collatable */
        *collation = InvalidOid;
        break;
    }

    case PLPGSQL_DTYPE_RECFIELD:
    {
        PLpgSQL_recfield *recfield = (PLpgSQL_recfield *)datum;
        PLpgSQL_rec *rec;

        rec = (PLpgSQL_rec *)(estate->datums[recfield->recparentno]);

        /*
				 * If record variable is NULL, instantiate it if it has a
				 * named composite type, else complain.  (This won't change
				 * the logical state of the record: it's still NULL.)
				 */
        if (rec->erh == NULL)
            instantiate_empty_record_variable(estate, rec);

        /*
				 * Look up the field's properties if we have not already, or
				 * if the tuple descriptor ID changed since last time.
				 */
        if (unlikely(recfield->rectupledescid != rec->erh->er_tupdesc_id))
        {
            recfield->rectupledescid = rec->erh->er_tupdesc_id;
        }

        *typeId = recfield->finfo.ftypeid;
        *typMod = recfield->finfo.ftypmod;
        *collation = recfield->finfo.fcollation;
        break;
    }

    default:
        elog(ERROR, "unrecognized dtype: %d", datum->dtype);
        *typeId = InvalidOid; /* keep compiler quiet */
        *typMod = -1;
        *collation = InvalidOid;
        break;
    }
}

/* ----------
 * exec_eval_integer		Evaluate an expression, coerce result to int4
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.  (We do this because the caller may be holding the
 * results of other, pass-by-reference, expression evaluations, such as
 * an array value to be subscripted.)
 * ----------
 */
static int
exec_eval_integer(PLpgSQL_execstate *estate,
                  PLpgSQL_expr *expr,
                  bool *isNull)
{
    Datum exprdatum;
    Oid exprtypeid;
    int32 exprtypmod;

    exprdatum = exec_eval_expr(estate, expr, NULL, isNull, &exprtypeid, &exprtypmod);
    exprdatum = exec_cast_value(estate, exprdatum, isNull,
                                exprtypeid, exprtypmod,
                                INT4OID, -1);
    return DatumGetInt32(exprdatum);
}

/* ----------
 * exec_eval_boolean		Evaluate an expression, coerce result to bool
 *
 * Note we do not do exec_eval_cleanup here; the caller must do it at
 * some later point.
 * ----------
 */
static bool
exec_eval_boolean(PLpgSQL_execstate *estate,
                  PLpgSQL_expr *expr,
                  bool *isNull)
{
    Datum exprdatum;
    // Oid exprtypeid;
    // int32 exprtypmod;
    exec_prepare_plan(estate, expr, 0,true);
    char * errorMsg = "";
    exprdatum = BepiExecBoolExpr(estate->handler, expr->planIndex,&errorMsg);
    if (errorMsg!=NULL && strlen(errorMsg)!=0){
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(errorMsg)));
    }
    *isNull = false;
    return DatumGetBool(exprdatum);
}

/* ----------
 * exec_eval_exception_var		Evaluete an exception expresion only like select sqlerrm or select sqlstate 
 * ----------
 * */
static Datum
exec_eval_exception_var(
        PLpgSQL_execstate *estate,
        PLpgSQL_expr *expr,
        bool *isNull,
        Oid *rettype,
        bool *finish
        )
{
    char *query = (char *)palloc(strlen(expr->query));
    strncpy(query, expr->query, strlen(expr->query));
    for (int i = 0 ; i < strlen(query); i++) {
        query[i] = (char)tolower((int)query[i]);
    }
    if (strcmp(query, "select sqlstate") == 0){
        // strictly equal with sqlstate 
        for (int i = 0; i < estate->ndatums; i++) {
            if (estate->datums[i]->dtype == PLPGSQL_DTYPE_VAR) {
                // this datum is a value 
                PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[i];
                if (strcmp(var->refname, "sqlstate") == 0) {
                    *finish = 1;
                    *isNull = var->isnull;
                    *rettype = var->datatype->typoid;
                    return var->isnull ? (Datum)NULL:var->value;
                }
            }
        }
    } else if (strcmp(query, "select sqlerrm") == 0){
        // strictly equal with sqlstate
        for (int i = 0; i < estate->ndatums; i++) {
            if (estate->datums[i]->dtype == PLPGSQL_DTYPE_VAR) {
                // this datum is a value 
                PLpgSQL_var *var = (PLpgSQL_var *) estate->datums[i];
                if (strcmp(var->refname, "sqlerrm") == 0) {
                    *finish = 1;
                    *isNull = var->isnull;
                    *rettype = var->datatype->typoid;
                    return var->isnull ? (Datum)NULL:var->value;
                }
            }
        }
    }
    *finish = 0;
    return 0;            
}
/* ----------
 * exec_eval_expr			Evaluate an expression and return
 *					the result Datum, along with data type/typmod.
 *
 * NOTE: caller must do exec_eval_cleanup when done with the Datum.
 * ----------
 */
static Datum
exec_eval_expr(
        PLpgSQL_execstate *estate,
        PLpgSQL_expr *expr,
        PLpgSQL_datum *target,
        bool *isNull, Oid *rettype,
        int32 *rettypmod)
{
    Datum result = 0;
    int rc;
    // Form_pg_attribute attr;
    int colNum = 0;
    bool finish;
    result = exec_eval_exception_var(estate, expr, isNull, rettype, &finish);
    // 如果求exception value, 的值直接返回 
    if (finish) return result;
    if (expr->planIndex == BEPI_NOCACHED_PLAN)
        exec_prepare_plan(estate, expr, CURSOR_OPT_PARALLEL_OK, true);
    /*
	 * If this is a simple expression, bypass SPI and use the executor
	 * directly
	 */
    PLSQLData* pData = PLSQL_GetCurrentThread_Data();
    if (pData)
    {
        char *varName = palloc(64);
        int varSN = -1;
        if (target != NULL && target->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var *var = (PLpgSQL_var *)(target);
            strcpy(varName, var->refname);
            varSN = var->dno;
        }
	    if (BEPI_exec_eval_expr(pData->handler, expr->planIndex, varName, varSN, &result,
                                rettype, expr->in_using, isNull))
	    {
	        return result;
	    }
    }
    /*
	 * Else do it the hard way via exec_run_select
	 */
    rc = exec_run_select(estate, expr, 2, NULL);
    if (rc != SPI_OK_SELECT)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("query \"%s\" did not return data", expr->query)));
    /*
	 * Check that the expression returns exactly one column...
	 */
	colNum = GetReturnColNum(estate->handler);
    if (colNum != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("query \"%s\" returned %d columns", expr->query, colNum)));
    }
	/* If there are no rows selected, the result is a NULL of that type.
	 */
    if (estate->eval_processed == 0)
    {
        // *isNull = true;
        return (Datum)0;
    }
    /*
	 * Check that the expression returned no more than one row.
	 */
    if (estate->eval_processed != 1)
        ereport(ERROR,
                (errcode(ERRCODE_CARDINALITY_VIOLATION),
                 errmsg("query \"%s\" returned more than one row",
                        expr->query)));
    /*
	 * Return the single result Datum.
	 */
    if (estate->eval_tuptable == NULL) {
        *isNull = false;
        return (Datum)0;
    }
    return SPI_getbinval(estate->eval_tuptable->vals[0],
                         estate->eval_tuptable->tupdesc, 1, isNull);
}

/* ----------
 * exec_run_select			Execute a select query
 * ----------
 */
static int
exec_run_select(PLpgSQL_execstate *estate,
                PLpgSQL_expr *expr, long maxtuples, Portal *portalP)
{
    ParamListInfo paramLI;
    int rc;
    bool is_query_or_hasReturning = false;

    /*
	 * On the first call for this expression generate the plan.
	 *
	 * If we don't need to return a portal, then we're just going to execute
	 * the query once, which means it's OK to use a parallel plan, even if the
	 * number of rows being fetched is limited.  If we do need to return a
	 * portal, the caller might do cursor operations, which parallel query
	 * can't support.
	 */
    if (expr->planIndex == BEPI_NOCACHED_PLAN)
        exec_prepare_plan(estate, expr,
                          portalP == NULL ? CURSOR_OPT_PARALLEL_OK : 0, true);

    /*
	 * Set up ParamListInfo to pass to executor
	 */
    paramLI = setup_param_list(estate, expr);

    /*
	 * If a portal was requested, put the query and paramlist into the portal
	 */
    if (portalP != NULL)
    {
        *portalP = SPI_cursor_open_with_paramlist(NULL, expr->plan,
                                                  paramLI,
                                                  estate->readonly_func);
        if (*portalP == NULL)
            elog(ERROR, "could not open implicit cursor for query \"%s\": %s",
                 expr->query, SPI_result_code_string(0));
        exec_eval_cleanup(estate);
        return SPI_OK_CURSOR;
    }

    /*
	 * Execute the query
	 */
    rc = BEPI_execute_plan_with_paramlist(estate->handler, expr->planIndex, paramLI,
                                          estate->readonly_func, maxtuples, &is_query_or_hasReturning);
    if (rc != SPI_OK_SELECT) //
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("query \"%s\" is not a SELECT", expr->query)));

    /* Save query results for eventual cleanup */
    Assert(estate->eval_tuptable == NULL);
    estate->eval_tuptable = SPI_tuptable;
    estate->eval_processed = SPI_processed;

    return rc;
}

#define INVALID_TUPLEDESC_IDENTIFIER ((uint64)1)
/*
 * exec_for_query --- execute body of FOR loop for each row from a portal
 *
 * Used by exec_stmt_fors, exec_stmt_forc and exec_stmt_dynfors
 */
static int
exec_for_query(PLpgSQL_execstate *estate, PLpgSQL_stmt_forq *stmt,
               Portal portal, bool prefetch_ok)
{
    PLpgSQL_variable *var;
    SPITupleTable *tuptab;
    bool found = false;
    int rc = PLPGSQL_RC_OK;
    uint64 previous_id = INVALID_TUPLEDESC_IDENTIFIER;
    bool tupdescs_match = true;
    uint64 n;

    /* Fetch loop variable's datum entry */
    var = (PLpgSQL_variable *)estate->datums[stmt->var->dno];

    /*
	 * Make sure the portal doesn't get closed by the user statements we
	 * execute.
	 */

    /*
	 * Fetch the initial tuple(s).  If prefetching is allowed then we grab a
	 * few more rows to avoid multiple trips through executor startup
	 * overhead.
	 */
    SPI_cursor_fetch(portal, true, prefetch_ok ? 10 : 1);
    tuptab = SPI_tuptable;
    n = SPI_processed;

    /*
	 * If the query didn't return any rows, set the target to NULL and fall
	 * through with found = false.
	 */
    if (n == 0)
    {
        exec_move_row(estate, var, NULL, tuptab->tupdesc, 0);
        exec_eval_cleanup(estate);
    }
    else
        found = true; /* processed at least one tuple */

    /*
	 * Now do the loop
	 */
    while (n > 0)
    {
        uint64 i;

        for (i = 0; i < n; i++)
        {
            /*
			 * Assign the tuple to the target.  Here, because we know that all
			 * loop iterations should be assigning the same tupdesc, we can
			 * optimize away repeated creations of expanded records with
			 * identical tupdescs.  Testing for changes of er_tupdesc_id is
			 * reliable even if the loop body contains assignments that
			 * replace the target's value entirely, because it's assigned from
			 * a process-global counter.  The case where the tupdescs don't
			 * match could possibly be handled more efficiently than this
			 * coding does, but it's not clear extra effort is worthwhile.
			 */
            if (var->dtype == PLPGSQL_DTYPE_REC)
            {
                PLpgSQL_rec *rec = (PLpgSQL_rec *)var;

                if (rec->erh &&
                    rec->erh->er_tupdesc_id == previous_id &&
                    tupdescs_match)
                {
                    /* Only need to assign a new tuple value */
                    /*expanded_record_set_tuple(rec->erh, tuptab->vals[i],*/
                    /*true, !estate->atomic);*/
                }
                else
                {
                    /*
					 * First time through, or var's tupdesc changed in loop,
					 * or we have to do it the hard way because type coercion
					 * is needed.
					 */
                    exec_move_row(estate, var,
                                  tuptab->vals[i], tuptab->tupdesc, 0);

                    /*
					 * Check to see if physical assignment is OK next time.
					 * Once the tupdesc comparison has failed once, we don't
					 * bother rechecking in subsequent loop iterations.
					 */
                    previous_id = rec->erh->er_tupdesc_id;
                }
            }
            else
                exec_move_row(estate, var, tuptab->vals[i], tuptab->tupdesc, 0);

            exec_eval_cleanup(estate);

            /*
			 * Execute the statements
			 */
            rc = exec_stmts(estate, stmt->body);

            LOOP_RC_PROCESSING(stmt->label, goto loop_exit);
        }

        SPI_freetuptable(tuptab);

        /*
		 * Fetch more tuples.  If prefetching is allowed, grab 50 at a time.
		 */
        SPI_cursor_fetch(portal, true, prefetch_ok ? 50 : 1);
        tuptab = SPI_tuptable;
        n = SPI_processed;
    }

loop_exit:

    /*
	 * Release last group of tuples (if any)
	 */
    SPI_freetuptable(tuptab);

    /*UnpinPortal(portal);*/

    /*
	 * Set the FOUND variable to indicate the result of executing the loop
	 * (namely, whether we looped one or more times). This must be set last so
	 * that it does not interfere with the value of the FOUND variable inside
	 * the loop processing itself.
	 */
    exec_set_found(estate, found);

    return rc;
}

#define InvalidLocalTransactionId 0

/*
 * Create a ParamListInfo to pass to SPI
 *
 * We use a single ParamListInfo struct for all SPI calls made from this
 * estate; it contains no per-param data, just hook functions, so it's
 * effectively read-only for SPI.
 *
 * An exception from pure read-only-ness is that the parserSetupArg points
 * to the specific PLpgSQL_expr being evaluated.  This is not an issue for
 * statement-level callers, but lower-level callers must save and restore
 * estate->paramLI->parserSetupArg just in case there's an active evaluation
 * at an outer call level.  (A plausible alternative design would be to
 * create a ParamListInfo struct for each PLpgSQL_expr, but for the moment
 * that seems like a waste of memory.)
 */
static ParamListInfo
setup_param_list(PLpgSQL_execstate *estate, PLpgSQL_expr *expr)
{
    ParamListInfo paramLI;

    /*
	 * We must have created the SPIPlan already (hence, query text has been
	 * parsed/analyzed at least once); else we cannot rely on expr->paramnos.
	 */
    Assert(expr->plan != NULL);

    /*
	 * We only need a ParamListInfo if the expression has parameters.  In
	 * principle we should test with bms_is_empty(), but we use a not-null
	 * test because it's faster.  In current usage bits are never removed from
	 * expr->paramnos, only added, so this test is correct anyway.
	 */
    if (expr->paramnos)
    {
        /* Use the common ParamListInfo */
        paramLI = estate->paramLI;

        /*
		 * Set up link to active expr where the hook functions can find it.
		 * Callers must save and restore parserSetupArg if there is any chance
		 * that they are interrupting an active use of parameters.
		 */
        paramLI->parserSetupArg = (void *)expr;

        /*
		 * Also make sure this is set before parser hooks need it.  There is
		 * no need to save and restore, since the value is always correct once
		 * set.  (Should be set already, but let's be sure.)
		 */
        expr->func = estate->func;
    }
    else
    {
        /*
		 * Expression requires no parameters.  Be sure we represent this case
		 * as a NULL ParamListInfo, so that plancache.c knows there is no
		 * point in a custom plan.
		 */
        paramLI = NULL;
    }
    return paramLI;
}

/*
 *
 * tup and tupdesc may both be NULL if we're just assigning an indeterminate
 * composite NULL to the target.  Alternatively, can have tup be NULL and
 * tupdesc not NULL, in which case we assign a row of NULLs to the target.
 *
 * Since this uses the mcontext for workspace, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 */

static void
exec_move_row(
        PLpgSQL_execstate *estate,
        PLpgSQL_variable *target,
        HeapTuple tup,
        TupleDesc tupdesc,
        bool inFetch)
{
    ExpandedRecordHeader *newerh = NULL;

    /*
	 * If target is RECORD, we may be able to avoid field-by-field processing.
	 */
    if (target->dtype == PLPGSQL_DTYPE_REC)
    {
        PLpgSQL_rec *rec = (PLpgSQL_rec *)target;

        /*
		 * If we have no source tupdesc, just set the record variable to NULL.
		 * (If we have a source tupdesc but not a tuple, we'll set the
		 * variable to a row of nulls, instead.  This is odd perhaps, but
		 * backwards compatible.)
		 */
        if (tupdesc == NULL)
        {
            if (rec->datatype &&
                rec->datatype->typtype == TYPTYPE_DOMAIN)
            {
                /*
				 * If it's a composite domain, NULL might not be a legal
				 * value, so we instead need to make an empty expanded record
				 * and ensure that domain type checking gets done.  If there
				 * is already an expanded record, piggyback on its lookups.
				 */
                newerh = make_expanded_record_for_rec(estate, rec,
                                                      NULL, rec->erh);
                /*expanded_record_set_tuple(newerh, NULL, false, false);*/
                assign_record_var(estate, rec, newerh, inFetch);
            }
            else
            {
                rec->erh = NULL;
            }
            return;
        }

        /*
		 * Build a new expanded record with appropriate tupdesc.
		 */

        /*
		 * If the rowtypes match, or if we have no tuple anyway, we can
		 * complete the assignment without field-by-field processing.
		 *
		 * The tests here are ordered more or less in order of cheapness.  We
		 * can easily detect it will work if the target is declared RECORD or
		 * has the same typeid as the source.  But when assigning from a query
		 * result, it's common to have a source tupdesc that's labeled RECORD
		 * but is actually physically compatible with a named-composite-type
		 * target, so it's worth spending extra cycles to check for that.
		 */
        if (rec->rectypeid == RECORDOID ||
            rec->rectypeid == tupdesc->tdtypeid ||
            compatible_tupdescs(tupdesc, NULL))
        {
            /* Complete the assignment */
            assign_record_var(estate, rec, newerh, inFetch);
            return;
        }
    }

    /*
	 * Otherwise, deconstruct the tuple and do field-by-field assignment,
	 * using exec_move_row_from_fields.
	 */
    if (tupdesc) // && HeapTupleIsValid(tup))
    {
        int td_natts = tupdesc->natts;
        Datum *values;
        bool *nulls;
        Datum values_local[64];
        bool nulls_local[64];

        /*
		 * Need workspace arrays.  If td_natts is small enough, use local
		 * arrays to save doing a palloc.  Even if it's not small, we can
		 * allocate both the Datum and isnull arrays in one palloc chunk.
		 */
        // todo(zh): force execute fllow if stmt
        if (td_natts <= lengthof(values_local) || true)
        {
            values = values_local;
            nulls = nulls_local;
        }
        else
        {
            char *chunk;

            chunk = eval_mcontext_alloc(estate,
                                        td_natts * (sizeof(Datum) + sizeof(bool)));
            values = (Datum *)chunk;
            nulls = (bool *)(chunk + td_natts * sizeof(Datum));
        }

        exec_move_row_from_fields(estate, target, newerh,
                                  values, nulls, tupdesc, inFetch);
    }
    else
    {
        /*
		 * Assign all-nulls.
		 */
        exec_move_row_from_fields(estate, target, newerh,
                                  NULL, NULL, NULL, inFetch);
    }
}

/*
 * Build an expanded record object suitable for assignment to "rec".
 *
 * Caller must supply either a source tuple descriptor or a source expanded
 * record (not both).  If the record variable has declared type RECORD,
 * it'll adopt the source's rowtype.  Even if it doesn't, we may be able to
 * piggyback on a source expanded record to save a typcache lookup.
 *
 * Caller must fill the object with data, then do assign_record_var().
 *
 * The new record is initially put into the mcontext, so it will be cleaned up
 * if we fail before reaching assign_record_var().
 */
static ExpandedRecordHeader *
make_expanded_record_for_rec(PLpgSQL_execstate *estate,
                             PLpgSQL_rec *rec,
                             TupleDesc srctupdesc,
                             ExpandedRecordHeader *srcerh)
{
    ExpandedRecordHeader *newerh = NULL;
    return newerh;
}

/*
 * exec_move_row_from_fields	Move arrays of field values into a record or row
 *
 * When assigning to a record, the caller must have already created a suitable
 * new expanded record object, newerh.  Pass NULL when assigning to a row.
 *
 * tupdesc describes the input row, which might have different column
 * types and/or different dropped-column positions than the target.
 * values/nulls/tupdesc can all be NULL if we just want to assign nulls to
 * all fields of the record or row.
 *
 * Since this uses the mcontext for workspace, caller should eventually call
 * exec_eval_cleanup to prevent long-term memory leaks.
 */
static void
exec_move_row_from_fields(PLpgSQL_execstate *estate, PLpgSQL_variable *target, ExpandedRecordHeader *newerh,
                          Datum *values, bool *nulls, TupleDesc tupdesc, bool inFetch)
{
    int td_natts = tupdesc ? tupdesc->natts : 0;
    int fnum;
    int anum;
    int strict_multiassignment_level = 0;

    /*
	 * The extra check strict strict_multi_assignment can be active, only when
	 * input tupdesc is specified.
	 */
    if (tupdesc != NULL)
    {
        if (plpgsql_extra_errors & PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT)
            strict_multiassignment_level = ERROR;
        else if (plpgsql_extra_warnings & PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT)
            strict_multiassignment_level = WARNING;
    }

    /* Handle RECORD-target case */
    if (target->dtype == PLPGSQL_DTYPE_REC)
    {
        PLpgSQL_rec *rec = (PLpgSQL_rec *)target;
        TupleDesc var_tupdesc = NULL;
        Datum newvalues_local[64];
        bool newnulls_local[64];

        Assert(newerh != NULL); /* caller must have built new object */

        /*
		 * Coerce field values if needed.  This might involve dealing with
		 * different sets of dropped columns and/or coercing individual column
		 * types.  That's sort of a pain, but historically plpgsql has allowed
		 * it, so we preserve the behavior.  However, it's worth a quick check
		 * to see if the tupdescs are identical.  (Since expandedrecord.c
		 * prefers to use refcounted tupdescs from the typcache, expanded
		 * records with the same rowtype will have pointer-equal tupdescs.)
		 */
        if (var_tupdesc != tupdesc)
        {
            int vtd_natts = var_tupdesc->natts;
            Datum *newvalues;
            bool *newnulls;

            /*
			 * Need workspace arrays.  If vtd_natts is small enough, use local
			 * arrays to save doing a palloc.  Even if it's not small, we can
			 * allocate both the Datum and isnull arrays in one palloc chunk.
			 */
            if (vtd_natts <= lengthof(newvalues_local))
            {
                newvalues = newvalues_local;
                newnulls = newnulls_local;
            }
            else
            {
                char *chunk;

                chunk = eval_mcontext_alloc(estate,
                                            vtd_natts * (sizeof(Datum) + sizeof(bool)));
                newvalues = (Datum *)chunk;
                newnulls = (bool *)(chunk + vtd_natts * sizeof(Datum));
            }

            /* Walk over destination columns */
            anum = 0;
            for (fnum = 0; fnum < vtd_natts; fnum++)
            {
                Form_pg_attribute attr = TupleDescAttr(var_tupdesc, fnum);
                Datum value;
                bool isnull;
                Oid valtype;
                int32 valtypmod;

                if (attr->attisdropped)
                {
                    /* expanded_record_set_fields should ignore this column */
                    continue; /* skip dropped column in record */
                }

                while (anum < td_natts &&
                       TupleDescAttr(tupdesc, anum)->attisdropped)
                    anum++; /* skip dropped column in tuple */

                if (anum < td_natts)
                {
                    value = values[anum];
                    isnull = nulls[anum];
                    valtype = TupleDescAttr(tupdesc, anum)->atttypid;
                    valtypmod = TupleDescAttr(tupdesc, anum)->atttypmod;
                    anum++;
                }
                else
                {
                    /* no source for destination column */
                    value = (Datum)0;
                    isnull = true;
                    valtype = UNKNOWNOID;
                    valtypmod = -1;

                    /* When source value is missing */
                    if (strict_multiassignment_level)
                        ereport(strict_multiassignment_level,
                                (errcode(ERRCODE_DATATYPE_MISMATCH),
                                 errmsg("number of source and target fields in assignment does not match"),
                                 /* translator: %s represents a name of an extra check */
                                 errdetail("%s check of %s is active.",
                                           "strict_multi_assignment",
                                           strict_multiassignment_level == ERROR ? "extra_errors" : "extra_warnings"),
                                 errhint("Make sure the query returns the exact list of columns.")));
                }

                /* Cast the new value to the right type, if needed. */
                newvalues[fnum] = exec_cast_value(estate,
                                                  value,
                                                  &isnull,
                                                  valtype,
                                                  valtypmod,
                                                  attr->atttypid,
                                                  attr->atttypmod);
                newnulls[fnum] = isnull;
            }

            /*
			 * When strict_multiassignment extra check is active, then ensure
			 * there are no unassigned source attributes.
			 */
            if (strict_multiassignment_level && anum < td_natts)
            {
                /* skip dropped columns in the source descriptor */
                while (anum < td_natts &&
                       TupleDescAttr(tupdesc, anum)->attisdropped)
                    anum++;

                if (anum < td_natts)
                    ereport(strict_multiassignment_level,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("number of source and target fields in assignment does not match"),
                             /* translator: %s represents a name of an extra check */
                             errdetail("%s check of %s is active.",
                                       "strict_multi_assignment",
                                       strict_multiassignment_level == ERROR ? "extra_errors" : "extra_warnings"),
                             errhint("Make sure the query returns the exact list of columns.")));
            }

            values = newvalues;
            nulls = newnulls;
        }

        /* Insert the coerced field values into the new expanded record */
        /*expanded_record_set_fields(newerh, values, nulls, !estate->atomic);*/

        /* Complete the assignment */
        assign_record_var(estate, rec, newerh, inFetch);

        return;
    }

    /* newerh should not have been passed in non-RECORD cases */
    Assert(newerh == NULL);

    /*
	 * For a row, we assign the individual field values to the variables the
	 * row points to.
	 *
	 * NOTE: both this code and the record code above silently ignore extra
	 * columns in the source and assume NULL for missing columns.  This is
	 * pretty dubious but it's the historical behavior.
	 *
	 * If we have no input data at all, we'll assign NULL to all columns of
	 * the row variable.
	 */
    if (target->dtype == PLPGSQL_DTYPE_ROW)
    {
        PLpgSQL_row *row = (PLpgSQL_row *)target;

        anum = 0;
        for (fnum = 0; fnum < row->nfields; fnum++)
        {
            PLpgSQL_var *var;
            Datum value;
            bool isnull;
            Oid valtype;
            int32 valtypmod;
            
            var = (PLpgSQL_var *)(estate->datums[row->varnos[fnum]]);
            if (strcmp(var->refname, "found") == 0)
            {
                continue;
            }

            while (anum < td_natts &&
                   TupleDescAttr(tupdesc, anum)->attisdropped)
                anum++; /* skip dropped column in tuple */

            if (anum < td_natts)
            {
                value = values[anum];
                isnull = nulls[anum];
                valtype = TupleDescAttr(tupdesc, anum)->atttypid;
                valtypmod = TupleDescAttr(tupdesc, anum)->atttypmod;
                anum++;
            }
            else
            {
                /* no source for destination column */
                value = (Datum)0;
                isnull = true;
                valtype = UNKNOWNOID;
                valtypmod = -1;

                if (strict_multiassignment_level)
                    ereport(strict_multiassignment_level,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("number of source and target fields in assignment does not match"),
                             /* translator: %s represents a name of an extra check */
                             errdetail("%s check of %s is active.",
                                       "strict_multi_assignment",
                                       strict_multiassignment_level == ERROR ? "extra_errors" : "extra_warnings"),
                             errhint("Make sure the query returns the exact list of columns.")));
            }
            exec_assign_value(estate, (PLpgSQL_datum *) var,
                              value, isnull, valtype, valtypmod, inFetch, var->dno, fnum, false, -1);
        }

        /*
		 * When strict_multiassignment extra check is active, ensure there are
		 * no unassigned source attributes.
		 */
        if (strict_multiassignment_level && anum < td_natts)
        {
            while (anum < td_natts &&
                   TupleDescAttr(tupdesc, anum)->attisdropped)
                anum++; /* skip dropped column in tuple */

            if (anum < td_natts)
                ereport(strict_multiassignment_level,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("number of source and target fields in assignment does not match"),
                         /* translator: %s represents a name of an extra check */
                         errdetail("%s check of %s is active.",
                                   "strict_multi_assignment",
                                   strict_multiassignment_level == ERROR ? "extra_errors" : "extra_warnings"),
                         errhint("Make sure the query returns the exact list of columns.")));
        }
        tupdesc->rocksDB_get = false;

        return;
    }

    elog(ERROR, "unsupported target type: %d", target->dtype);
}

/*
 * compatible_tupdescs: detect whether two tupdescs are physically compatible
 *
 * TRUE indicates that a tuple satisfying src_tupdesc can be used directly as
 * a value for a composite variable using dst_tupdesc.
 */
static bool
compatible_tupdescs(TupleDesc src_tupdesc, TupleDesc dst_tupdesc)
{
    int i;

    /* Possibly we could allow src_tupdesc to have extra columns? */
    if (dst_tupdesc->natts != src_tupdesc->natts)
        return false;

    for (i = 0; i < dst_tupdesc->natts; i++)
    {
        Form_pg_attribute dattr = TupleDescAttr(dst_tupdesc, i);
        Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, i);

        if (dattr->attisdropped != sattr->attisdropped)
            return false;
        if (!dattr->attisdropped)
        {
            /* Normal columns must match by type and typmod */
            if (dattr->atttypid != sattr->atttypid ||
                (dattr->atttypmod >= 0 &&
                 dattr->atttypmod != sattr->atttypmod))
                return false;
        }
        else
        {
            /* Dropped columns are OK as long as length/alignment match */
            if (dattr->attlen != sattr->attlen ||
                dattr->attalign != sattr->attalign)
                return false;
        }
    }
    return true;
}

/* ----------
 * make_tuple_from_row		Make a tuple from the values of a row object
 *
 * A NULL return indicates rowtype mismatch; caller must raise suitable error
 *
 * The result tuple is freshly palloc'd in caller's context.  Some junk
 * may be left behind in eval_mcontext, too.
 * ----------
 */
static HeapTuple
make_tuple_from_row(PLpgSQL_execstate *estate,
                    PLpgSQL_row *row,
                    TupleDesc tupdesc)
{
    int natts = tupdesc->natts;
    HeapTuple tuple = NULL;
    Datum *dvalues;
    bool *nulls;
    int i;

    if (natts != row->nfields)
        return NULL;

    dvalues = (Datum *)eval_mcontext_alloc0(estate, natts * sizeof(Datum));
    nulls = (bool *)eval_mcontext_alloc(estate, natts * sizeof(bool));

    for (i = 0; i < natts; i++)
    {
        Oid fieldtypeid;
        int32 fieldtypmod;

        if (TupleDescAttr(tupdesc, i)->attisdropped)
        {
            nulls[i] = true; /* leave the column as null */
            continue;
        }

        exec_eval_datum(estate, estate->datums[row->varnos[i]],
                        &fieldtypeid, &fieldtypmod,
                        &dvalues[i], &nulls[i]);
        if (fieldtypeid != TupleDescAttr(tupdesc, i)->atttypid)
            return NULL;
        /* XXX should we insist on typmod match, too? */
    }

    return tuple;
}

/*
 * exec_move_row_from_datum		Move a composite Datum into a record or row
 *
 * This is equivalent to deconstruct_composite_datum() followed by
 * exec_move_row(), but we can optimize things if the Datum is an
 * expanded-record reference.
 *
 * Note: it's caller's responsibility to be sure value is of composite type.
 */
static void
exec_move_row_from_datum(PLpgSQL_execstate *estate,
                         PLpgSQL_variable *target,
                         Datum value, bool inFetch)
{
    /* Check to see if source is an expanded record */
    if (VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(value)))
    {
        ExpandedRecordHeader *erh = NULL;
        ExpandedRecordHeader *newerh = NULL;

        Assert(erh->er_magic == ER_MAGIC);

        /* These cases apply if the target is record not row... */
        if (target->dtype == PLPGSQL_DTYPE_REC)
        {
            PLpgSQL_rec *rec = (PLpgSQL_rec *)target;

            /*
			 * If it's the same record already stored in the variable, do
			 * nothing.  This would happen only in silly cases like "r := r",
			 * but we need some check to avoid possibly freeing the variable's
			 * live value below.  Note that this applies even if what we have
			 * is a R/O pointer.
			 */
            if (erh == rec->erh)
                return;

            /*
			 * If we have a R/W pointer, we're allowed to just commandeer
			 * ownership of the expanded record.  If it's of the right type to
			 * put into the record variable, do that.  (Note we don't accept
			 * an expanded record of a composite-domain type as a RECORD
			 * value.  We'll treat it as the base composite type instead;
			 * compare logic in make_expanded_record_for_rec.)
			 */
            if (VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(value)) &&
                (rec->rectypeid == erh->er_decltypeid ||
                 (rec->rectypeid == RECORDOID &&
                  !(erh))))
            {
                assign_record_var(estate, rec, erh, inFetch);
                return;
            }

            /*
			 * If we already have an expanded record object in the target
			 * variable, and the source record contains a valid tuple
			 * representation with the right rowtype, then we can skip making
			 * a new expanded record and just assign the tuple with
			 * expanded_record_set_tuple.  (We can't do the equivalent if we
			 * have to do field-by-field assignment, since that wouldn't be
			 * atomic if there's an error.)  We consider that there's a
			 * rowtype match only if it's the same named composite type or
			 * same registered rowtype; checking for matches of anonymous
			 * rowtypes would be more expensive than this is worth.
			 */
            if (rec->erh &&
                (erh->flags & ER_FLAG_FVALUE_VALID) &&
                erh->er_typeid == rec->erh->er_typeid &&
                (erh->er_typeid != RECORDOID ||
                 (erh->er_typmod == rec->erh->er_typmod &&
                  erh->er_typmod >= 0)))
            {
                return;
            }

            /*
			 * Otherwise we're gonna need a new expanded record object.  Make
			 * it here in hopes of piggybacking on the source object's
			 * previous typcache lookup.
			 */
            newerh = make_expanded_record_for_rec(estate, rec, NULL, erh);

            /*
			 * If the expanded record contains a valid tuple representation,
			 * and we don't need rowtype conversion, then just copying the
			 * tuple is probably faster than field-by-field processing.  (This
			 * isn't duplicative of the previous check, since here we will
			 * catch the case where the record variable was previously empty.)
			 */
            if ((erh->flags & ER_FLAG_FVALUE_VALID) &&
                (rec->rectypeid == RECORDOID ||
                 rec->rectypeid == erh->er_typeid))
            {
                /*expanded_record_set_tuple(newerh, erh->fvalue,*/
                /*true, !estate->atomic);*/
                assign_record_var(estate, rec, newerh, inFetch);
                return;
            }

            /*
			 * Need to special-case empty source record, else code below would
			 * leak newerh.
			 */

            /* Set newerh to a row of NULLs */
            /*deconstruct_expanded_record(newerh);*/
            assign_record_var(estate, rec, newerh, inFetch);
            return;

        } /* end of record-target-only cases */

        /*
		 * If the source expanded record is empty, we should treat that like a
		 * NULL tuple value.  (We're unlikely to see such a case, but we must
		 * check this; deconstruct_expanded_record would cause a change of
		 * logical state, which is not OK.)
		 */
        return;
    }
    else
    {
        /*
		 * Nope, we've got a plain composite Datum.  Deconstruct it; but we
		 * don't use deconstruct_composite_datum(), because we may be able to
		 * skip calling lookup_rowtype_tupdesc().
		 */
        HeapTupleData tmptup;
        Oid tupType = 0;
        int32 tupTypmod = 0;
        TupleDesc tupdesc = NULL;
        MemoryContext oldcontext;

        /* Ensure that any detoasted data winds up in the eval_mcontext */
        oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));
        MemoryContextSwitchTo(oldcontext);

        /* Now, if the target is record not row, maybe we can optimize ... */
        if (target->dtype == PLPGSQL_DTYPE_REC)
        {
            PLpgSQL_rec *rec = (PLpgSQL_rec *)target;

            /*
			 * If we already have an expanded record object in the target
			 * variable, and the source datum has a matching rowtype, then we
			 * can skip making a new expanded record and just assign the tuple
			 * with expanded_record_set_tuple.  We consider that there's a
			 * rowtype match only if it's the same named composite type or
			 * same registered rowtype.  (Checking to reject an anonymous
			 * rowtype here should be redundant, but let's be safe.)
			 */
            if (rec->erh &&
                tupType == rec->erh->er_typeid &&
                (tupType != RECORDOID ||
                 (tupTypmod == rec->erh->er_typmod &&
                  tupTypmod >= 0)))
            {
                /*expanded_record_set_tuple(rec->erh, &tmptup,*/
                /*true, !estate->atomic);*/
                return;
            }

            /*
			 * If the source datum has a rowtype compatible with the target
			 * variable, just build a new expanded record and assign the tuple
			 * into it.  Using make_expanded_record_from_typeid() here saves
			 * one typcache lookup compared to the code below.
			 */
            if (rec->rectypeid == RECORDOID || rec->rectypeid == tupType)
            {
                ExpandedRecordHeader *newerh;
                MemoryContext mcontext = get_eval_mcontext(estate);

                newerh = make_expanded_record_from_typeid(tupType, tupTypmod,
                                                          mcontext);
                assign_record_var(estate, rec, newerh, inFetch);
                return;
            }

            /*
			 * Otherwise, we're going to need conversion, so fall through to
			 * do it the hard way.
			 */
        }

        /*
		 * ROW target, or unoptimizable RECORD target, so we have to expend a
		 * lookup to obtain the source datum's tupdesc.
		 */

        /* Do the move */
        exec_move_row(estate, target, &tmptup, tupdesc, 0);
    }
}

/*
 * If we have not created an expanded record to hold the record variable's
 * value, do so.  The expanded record will be "empty", so this does not
 * change the logical state of the record variable: it's still NULL.
 * However, now we'll have a tupdesc with which we can e.g. look up fields.
 */
static void
instantiate_empty_record_variable(PLpgSQL_execstate *estate, PLpgSQL_rec *rec)
{
    Assert(rec->erh == NULL); /* else caller error */

    /* If declared type is RECORD, we can't instantiate */
    if (rec->rectypeid == RECORDOID)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("record \"%s\" is not assigned yet", rec->refname),
                 errdetail("The tuple structure of a not-yet-assigned record is indeterminate.")));

    /* OK, do it */
    rec->erh = make_expanded_record_from_typeid(rec->rectypeid, -1,
                                                estate->datum_context);
}

/* ----------
 * convert_value_to_string			Convert a non-null Datum to C string
 *
 * Note: the result is in the estate's eval_mcontext, and will be cleared
 * by the next exec_eval_cleanup() call.  The invoked output function might
 * leave additional cruft there as well, so just pfree'ing the result string
 * would not be enough to avoid memory leaks if we did not do it like this.
 * In most usages the Datum being passed in is also in that context (if
 * pass-by-reference) and so an exec_eval_cleanup() call is needed anyway.
 *
 * Note: not caching the conversion function lookup is bad for performance.
 * However, this function isn't currently used in any places where an extra
 * catalog lookup or two seems like a big deal.
 * ----------
 */
static char *
convert_value_to_string(PLpgSQL_execstate *estate, Datum value, Oid valtype)
{
    char *result = palloc(RESSIZE);
    MemSet(result, 0, RESSIZE);
    // MemoryContext oldcontext;
    // Oid typoutput;
    // bool typIsVarlena;

	switch (valtype) {
		case BOOLOID:{
			if DatumGetBool(value) {
				strcpy(result, "true");
			} else {
				strcpy(result, "false");
			}
			break;
		}
		case VARBITOID: {
			Pointer pointer = DatumGetPointer(value);
			strcpy(result, pointer);
			break;
		}
		case INT2OID:
		case INT4OID:
		case INT8OID: {
			int64 intVal = DatumGetInt64(value);
			sprintf(result, "%ld", intVal);
			break;
		}
		case FLOAT4OID: {
			float4 floatVal = DatumGetFloat4(value);
			sprintf(result, "%f", floatVal);
			break; 
		}
		case FLOAT8OID:
		case NUMERICOID: {
		        float8 floaf8Val = DatumGetFloat8(value);
		        sprintf(result, "%f", floaf8Val);
			break;
		}
		case VARCHAROID:
		case TEXTOID: {
		        char *str = DatumGetCString(value);
		        if (str[0] == '\'' && str[strlen(str) - 1] == '\'') {
		            str[strlen(str) - 1] = '\0';
		            str++;
		        }
		        sprintf(result, "%s", str);
			break;
		}
		case BYTEAOID: {
			result = DatumGetCString(value);
			break;
		}
		case DATEOID: {
			break;
		}
		case TIMEOID: {
			break;
		}
		case TIMESTAMPOID: {
			break;
		}
		case TIMESTAMPTZOID: {
			break;
		}
		case INTERVALOID: {
			break;
		}
		case JSONBOID: {
			break;
		}
		case UUIDOID: {
			break;
		}
		case INETOID: {
			break;
		}
		default:
            break;
	}

    return result;
}


static Datum
convert_string_to_value(PLpgSQL_execstate *estate, char* str, Oid valtype) {
	Datum res = (Datum)0;
	switch (valtype) {
		case BOOLOID:{
			if (strcmp(str, "true") == 0) {
				res = BoolGetDatum(true);
			} else {
				res = BoolGetDatum(false);
			}
			break;
		}
		case VARBITOID: {
			res = CStringGetDatum(str);
			break;
		}
		case INT2OID:
		case INT4OID:
		case INT8OID: {
			int64 intVal = atoi(str);
			res = DatumGetInt64(intVal);
			break;
		}
		case FLOAT4OID: {
			float4 floatVal = atof(str);
			res = Float4GetDatumFast(floatVal);
			break;
		}
		case FLOAT8OID:
		case NUMERICOID: {
			float8 floaf8Val = atof(str);
			res = Float8GetDatumFast(floaf8Val);
			break;
		}
		case VARCHAROID:
		case TEXTOID: {
			char *str2 = str;
			if (str2[0] == '\'' && str2[strlen(str2) - 1] == '\'') {
				str2[strlen(str2) - 1] = '\0';
				str2++;
			}
			res = CStringGetDatum(str2);
			break;
		}
		case BYTEAOID: {
			break;
		}
		case DATEOID: {
			break;
		}
		case TIMEOID: {
			break;
		}
		case TIMESTAMPOID: {
			break;
		}
		case TIMESTAMPTZOID: {
			break;
		}
		case INTERVALOID: {
			break;
		}
		case JSONBOID: {
			break;
		}
		case UUIDOID: {
			break;
		}
		case INETOID: {
			break;
		}
		default:
			ereport(ERROR, (errmsg("unImplement type %d", valtype)));
	}
    return res;
}

/* ----------
 * exec_cast_value			Cast a value if required
 *
 * Note that *isnull is an input and also an output parameter.  While it's
 * unlikely that a cast operation would produce null from non-null or vice
 * versa, that could happen in principle.
 *
 * Note: the estate's eval_mcontext is used for temporary storage, and may
 * also contain the result Datum if we have to do a conversion to a pass-
 * by-reference data type.  Be sure to do an exec_eval_cleanup() call when
 * done with the result.
 * ----------
 */
static Datum
exec_cast_value(PLpgSQL_execstate *estate,
                Datum value, bool *isnull,
                Oid valtype, int32 valtypmod,
                Oid reqtype, int32 reqtypmod)
{
    /*
	 * If the type of the given value isn't what's requested, convert it.
	 */
    // Datum result;
    if (valtype != reqtype ||
        (valtypmod != reqtypmod && reqtypmod != -1))
    {
    	char *srcStr = convert_value_to_string(estate, value, valtype);

    	char *resStr = NULL;
    	resStr = BEPI_castValue_toType(estate->handler, srcStr, &resStr, valtype, reqtype);
    	Datum result = convert_string_to_value(estate, resStr, reqtype);
    	return result;
    }
    return value;
}

/*
 * exec_check_rw_parameter --- can we pass expanded object as read/write param?
 *
 * If we have an assignment like "x := array_append(x, foo)" in which the
 * top-level function is trusted not to corrupt its argument in case of an
 * error, then when x has an expanded object as value, it is safe to pass the
 * value as a read/write pointer and let the function modify the value
 * in-place.
 *
 * This function checks for a safe expression, and sets expr->rwparam to the
 * dno of the target variable (x) if safe, or -1 if not safe.
 */
static void
exec_check_rw_parameter(PLpgSQL_expr *expr, int target_dno)
{
    List *fargs = NULL;
    ListCell *lc;

    /* Assume unsafe */
    expr->rwparam = -1;

    /*
	 * If the expression isn't simple, there's no point in trying to optimize
	 * (because the exec_run_select code path will flatten any expanded result
	 * anyway).  Even without that, this seems like a good safety restriction.
	 */
    if (expr->expr_simple_expr == NULL)
    return;

    /*
	 * The target variable (in the form of a Param) must only appear as a
	 * direct argument of the top-level function.
	 */
    foreach (lc, fargs)
    {
        Node *arg = (Node *)lfirst(lc);

        /* A Param is OK, whether it's the target variable or not */
        if (arg && IsA(arg, Param))
            continue;
        /* Otherwise, argument expression must not reference target */
        if (contains_target_param(arg, &target_dno))
            return;
    }

    /* OK, we can pass target as a read-write parameter */
    expr->rwparam = target_dno;
}

/*
 * Recursively check for a Param referencing the target variable
 */
static bool
contains_target_param(Node *node, int *target_dno)
{
    if (node == NULL)
        return false;
    if (IsA(node, Param))
    {
        Param *param = (Param *)node;

        if (param->paramkind == PARAM_EXTERN &&
            param->paramid == *target_dno + 1)
            return true;
        return false;
    }
    return true;
}

/* ----------
 * exec_set_found			Set the global found variable to true/false
 * ----------
 */
static void
exec_set_found(PLpgSQL_execstate *estate, bool state)
{
    PLpgSQL_var *var;
    var = (PLpgSQL_var *)(estate->datums[estate->found_varno]);
    assign_simple_var(estate, var, false, false);
}

/*
 * plpgsql_create_econtext --- create an eval_econtext for the current function
 *
 * We may need to create a new shared_simple_eval_estate too, if there's not
 * one already for the current transaction.  The EState will be cleaned up at
 * transaction end.
 */
static void
plpgsql_create_econtext(PLpgSQL_execstate *estate)
{
    SimpleEcontextStackEntry *entry;

    /*
	 * Create an EState for evaluation of simple expressions, if there's not
	 * one already in the current transaction.  The EState is made a child of
	 * TopTransactionContext so it will have the right lifespan.
	 *
	 * Note that this path is never taken when executing a DO block; the
	 * required EState was already made by plpgsql_inline_handler.
	 */
    if (estate->simple_eval_estate == NULL)
    {
        MemoryContext oldcontext;

        if (shared_simple_eval_estate == NULL)
        {
            oldcontext = MemoryContextSwitchTo(TopTransactionContext);
            MemoryContextSwitchTo(oldcontext);
        }
        estate->simple_eval_estate = shared_simple_eval_estate;
    }
    /*
	 * Make a stack entry so we can clean up the econtext at subxact end.
	 * Stack entries are kept in TopTransactionContext for simplicity.
	 */
    if (TopTransactionContext != NULL)
    {
        entry = (SimpleEcontextStackEntry *)
            MemoryContextAlloc(TopTransactionContext,
                               sizeof(SimpleEcontextStackEntry));

        entry->stack_econtext = estate->eval_econtext;

        entry->next = simple_econtext_stack;
        simple_econtext_stack = entry;
    }
}

/*
 * plpgsql_destroy_econtext --- destroy function's econtext
 *
 * We check that it matches the top stack entry, and destroy the stack
 * entry along with the context.
 */
static void
plpgsql_destroy_econtext(PLpgSQL_execstate *estate)
{
    SimpleEcontextStackEntry *next;

    Assert(simple_econtext_stack != NULL);
    Assert(simple_econtext_stack->stack_econtext == estate->eval_econtext);
    if (simple_econtext_stack != NULL)
    {
        next = simple_econtext_stack->next;
        pfree(simple_econtext_stack);
        simple_econtext_stack = next;
    }
    estate->eval_econtext = NULL;
}

/*
 * assign_simple_var --- assign a new value to any VAR datum.
 *
 * This should be the only mechanism for assignment to simple variables,
 * lest we do the release of the old value incorrectly (not to mention
 * the detoasting business).
 */
static void
assign_simple_var(PLpgSQL_execstate *estate, PLpgSQL_var *var,
                 bool isnull, bool freeable)
{
    Assert(var->dtype == PLPGSQL_DTYPE_VAR ||
           var->dtype == PLPGSQL_DTYPE_PROMISE);

    /*
	 * In non-atomic contexts, we do not want to store TOAST pointers in
	 * variables, because such pointers might become stale after a commit.
	 * Forcibly detoast in such cases.  We don't want to detoast (flatten)
	 * expanded objects, however; those should be OK across a transaction
	 * boundary since they're just memory-resident objects.  (Elsewhere in
	 * this module, operations on expanded records likewise need to request
	 * detoasting of record fields when !estate->atomic.  Expanded arrays are
	 * not a problem since all array entries are always detoasted.)
	 */
    if (!estate->atomic && !isnull && var->datatype->typlen == -1)
    {
        MemoryContext oldcxt;

        /*
		 * Do the detoasting in the eval_mcontext to avoid long-term leakage
		 * of whatever memory toast fetching might leak.  Then we have to copy
		 * the detoasted datum to the function's main context, which is a
		 * pain, but there's little choice.
		 */
        oldcxt = MemoryContextSwitchTo(get_eval_mcontext(estate));
        MemoryContextSwitchTo(oldcxt);
        /* Now's a good time to not leak the input value if it's freeable */
        /* Once we copy the value, it's definitely freeable */
        freeable = true;
        /* Can't clean up eval_mcontext here, but it'll happen before long */
    }

    /* Free the old value if needed */
    if (var->freeval)
    {
        pfree(DatumGetPointer(var->value));
    }
    /* Assign new value to datum */
    var->isnull = isnull;
    var->freeval = freeable;

    /*
	 * If it's a promise variable, then either we just assigned the promised
	 * value, or the user explicitly assigned an overriding value.  Either
	 * way, cancel the promise.
	 */
    var->promise = PLPGSQL_PROMISE_NONE;
}

/*
 * free old value of a text variable and assign new value from C string
 */
static void
assign_text_var(PLpgSQL_execstate *estate, PLpgSQL_var *var, const char *str)
{
    assign_simple_var(estate, var, false, true);
}

/*
 * assign_record_var --- assign a new value to any REC datum.
 */
static void
assign_record_var(PLpgSQL_execstate *estate, PLpgSQL_rec *rec,
                  ExpandedRecordHeader *erh, bool inFetch)
{
    Assert(rec->dtype == PLPGSQL_DTYPE_REC);
    /* ... and install the new */
    char *err = AssignTheValue(estate->handler, rec->dno, -1, inFetch, false, -1);
    if (strlen(err) != 0) {
        ereport(ERROR, (errmsg("%s", err)));
    }
}

/*
 * exec_eval_using_params --- evaluate params of USING clause
 *
 * The result data structure is created in the stmt_mcontext, and should
 * be freed by resetting that context.
 */
static PreparedParamsData *
exec_eval_using_params(PLpgSQL_execstate *estate, List *params)
{
    PreparedParamsData *ppd;
    MemoryContext stmt_mcontext = get_stmt_mcontext(estate);
    int nargs;
    int i;
    ListCell *lc;

    ppd = (PreparedParamsData *)
        MemoryContextAlloc(stmt_mcontext, sizeof(PreparedParamsData));
    nargs = list_length(params);

    ppd->nargs = nargs;
    ppd->types = (Oid *)
        MemoryContextAlloc(stmt_mcontext, nargs * sizeof(Oid));
    ppd->values = (Datum *)
        MemoryContextAlloc(stmt_mcontext, nargs * sizeof(Datum));
    ppd->nulls = (char *)
        MemoryContextAlloc(stmt_mcontext, nargs * sizeof(char));

    i = 0;
    foreach (lc, params)
    {
        PLpgSQL_expr *param = (PLpgSQL_expr *)lfirst(lc);
        bool isnull;
        int32 ppdtypmod;
        MemoryContext oldcontext;
        param->in_using = true;
        ppd->values[i] = exec_eval_expr(estate, param, NULL,
                                        &isnull,
                                        &ppd->types[i],
                                        &ppdtypmod);
        ppd->nulls[i] = isnull ? 'n' : ' ';

        oldcontext = MemoryContextSwitchTo(stmt_mcontext);

        if (ppd->types[i] == UNKNOWNOID)
        {
            /*
			 * Treat 'unknown' parameters as text, since that's what most
			 * people would expect. SPI_execute_with_args can coerce unknown
			 * constants in a more intelligent way, but not unknown Params.
			 * This code also takes care of copying into the right context.
			 * Note we assume 'unknown' has the representation of C-string.
			 */
            ppd->types[i] = TEXTOID;
        }
        /* pass-by-ref non null values must be copied into stmt_mcontext */
        else if (!isnull)
        {
            /*get_typlenbyval(ppd->types[i], &typLen, &typByVal);*/
            /*if (!typByVal)*/
            /*ppd->values[i] = datumCopy(ppd->values[i], typByVal, typLen);*/
        }

        MemoryContextSwitchTo(oldcontext);

        exec_eval_cleanup(estate);

        i++;
    }

    return ppd;
}

/*
 * Open portal for dynamic query
 *
 * Caution: this resets the stmt_mcontext at exit.  We might eventually need
 * to move that responsibility to the callers, but currently no caller needs
 * to have statement-lifetime temp data that survives past this, so it's
 * simpler to do it here.
 */
static Portal
exec_dynquery_with_params(PLpgSQL_execstate *estate,
                          PLpgSQL_expr *dynquery,
                          List *params,
                          const char *portalname,
                          int cursorOptions)
{
    Portal portal;
    Datum query;
    bool isnull;
    Oid restype;
    int32 restypmod;
    char *querystr;
    MemoryContext stmt_mcontext = get_stmt_mcontext(estate);

    /*
	 * Evaluate the string expression after the EXECUTE keyword. Its result is
	 * the querystring we have to execute.
	 */
    query = exec_eval_expr(estate, dynquery, NULL, &isnull, &restype, &restypmod);
    if (isnull)
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("query string argument of EXECUTE is null")));

    /* Get the C-String representation */
    querystr = convert_value_to_string(estate, query, restype);

    /* copy it into the stmt_mcontext before we clean up */
    querystr = MemoryContextStrdup(stmt_mcontext, querystr);

    exec_eval_cleanup(estate);

    /*
	 * Open an implicit cursor for the query.  We use
	 * SPI_cursor_open_with_args even when there are no params, because this
	 * avoids making and freeing one copy of the plan.
	 */
    if (params)
    {
        PreparedParamsData *ppd;

        ppd = exec_eval_using_params(estate, params);
        portal = SPI_cursor_open_with_args(portalname,
                                           querystr,
                                           ppd->nargs, ppd->types,
                                           ppd->values, ppd->nulls,
                                           estate->readonly_func,
                                           cursorOptions);
    }
    else
    {
        portal = SPI_cursor_open_with_args(portalname,
                                           querystr,
                                           0, NULL,
                                           NULL, NULL,
                                           estate->readonly_func,
                                           cursorOptions);
    }

    if (portal == NULL)
        elog(ERROR, "could not open implicit cursor for query \"%s\": %s",
             querystr, SPI_result_code_string(0));

    /* Release transient data */
    MemoryContextReset(stmt_mcontext);

    return portal;
}

/*
 * Return a formatted string with information about an expression's parameters,
 * or NULL if the expression does not take any parameters.
 * The result is in the eval_mcontext.
 */
static char *
format_expr_params(PLpgSQL_execstate *estate,
                   const PLpgSQL_expr *expr)
{
    int paramno;
    int dno;
    StringInfoData paramstr;
    MemoryContext oldcontext;

    if (!expr->paramnos)
        return NULL;

    oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

    initStringInfo(&paramstr);
    paramno = 0;
    dno = -1;
    while (1)
    {
        Datum paramdatum;
        Oid paramtypeid;
        bool paramisnull;
        int32 paramtypmod;
        PLpgSQL_var *curvar;

        curvar = (PLpgSQL_var *)estate->datums[dno];

        exec_eval_datum(estate, (PLpgSQL_datum *)curvar,
                        &paramtypeid, &paramtypmod,
                        &paramdatum, &paramisnull);

        appendStringInfo(&paramstr, "%s%s = ",
                         paramno > 0 ? ", " : "",
                         curvar->refname);

        if (paramisnull)
            appendStringInfoString(&paramstr, "NULL");
        else
        {
            char *value = convert_value_to_string(estate, paramdatum, paramtypeid);
            char *p;

            appendStringInfoCharMacro(&paramstr, '\'');
            for (p = value; *p; p++)
            {
                if (*p == '\'') /* double single quotes */
                    appendStringInfoCharMacro(&paramstr, *p);
                appendStringInfoCharMacro(&paramstr, *p);
            }
            appendStringInfoCharMacro(&paramstr, '\'');
        }

        paramno++;
    }

    MemoryContextSwitchTo(oldcontext);

    return paramstr.data;
}

/*
 * Return a formatted string with information about PreparedParamsData, or NULL
 * if there are no parameters.
 * The result is in the eval_mcontext.
 */
static char *
format_preparedparamsdata(PLpgSQL_execstate *estate,
                          const PreparedParamsData *ppd)
{
    int paramno;
    StringInfoData paramstr;
    MemoryContext oldcontext;

    if (!ppd)
        return NULL;

    oldcontext = MemoryContextSwitchTo(get_eval_mcontext(estate));

    initStringInfo(&paramstr);
    for (paramno = 0; paramno < ppd->nargs; paramno++)
    {
        appendStringInfo(&paramstr, "%s$%d = ",
                         paramno > 0 ? ", " : "",
                         paramno + 1);

        if (ppd->nulls[paramno] == 'n')
            appendStringInfoString(&paramstr, "NULL");
        else
        {
            char *value = convert_value_to_string(estate, ppd->values[paramno], ppd->types[paramno]);
            char *p;

            appendStringInfoCharMacro(&paramstr, '\'');
            for (p = value; *p; p++)
            {
                if (*p == '\'') /* double single quotes */
                    appendStringInfoCharMacro(&paramstr, *p);
                appendStringInfoCharMacro(&paramstr, *p);
            }
            appendStringInfoCharMacro(&paramstr, '\'');
        }
    }

    MemoryContextSwitchTo(oldcontext);

    return paramstr.data;
}

TupleDesc makeEmptyTupleDesc(CursorFetchPre cfp) {
    TupleDesc tupleDesc = (TupleDesc)palloc(sizeof(TupleDescData));
    tupleDesc->rocksDB_get = false;
    tupleDesc->natts = cfp.colNum;
    tupleDesc->tdrefcount = 0;
    tupleDesc->tdtypeid = 0;
    tupleDesc->tdtypmod = 0;

    TupleConstr tempConstr;
    tupleDesc->constr = &tempConstr;
    // tupleDesc->constr.

    int i;
    for (i = 0; i < cfp.colNum; i++) {
        tupleDesc->attrs[i].atttypid = cfp.colTypes[i];
        tupleDesc->attrs[i].attisdropped = false;
    }
    return tupleDesc;
}

char* ReplaceSubStr(const char* str, const char* srcSubStr, const char* dstSubStr, char* out)
{
    if (!str&&!out)
    {
        return NULL;
    }
    if (!srcSubStr && !dstSubStr)
    {
        return out;
    }
    char *out_temp = out;
    int src_len = strlen(srcSubStr);
    int dst_len = strlen(dstSubStr);
    while (*str!='\0')
    {
        if (*str == *srcSubStr)
        {
            //可能发生替换
            const char* str_temp = str;
            int flag = 0;
            for (int i = 0; i < src_len; i++)
            {
                if (str_temp[i]!=srcSubStr[i])
                {
                    flag = 1;
                    break;
                }
            }
            if (flag)
            {
                //说明不用替换
                *out_temp++ = *str++;
            }
            else
            {
                //说明要替换
                for (int i = 0; i < dst_len; i++)
                {
                    *out_temp++ = dstSubStr[i];
                }
                str = str + src_len;
            }
        }
        else
        {
            //不用替换
            *out_temp++ = *str++;
        }
    }
    *out_temp = 0;
    return out;
}


void add_ErrMsg(char * err ,const char * msg){
    strcat(err,"\n");
    strcat(err,msg);
}
