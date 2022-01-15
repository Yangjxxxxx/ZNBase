//
// Created by chenmingsong on 2020/5/13.
//

#ifndef PLSQL_ZNBasePARSENODES_H
#define PLSQL_ZNBasePARSENODES_H

// first used in pl_handle.c
typedef struct CallContext
{
    NodeTag type;
    bool atomic;
} CallContext;

typedef struct Node
{
    NodeTag type;
} Node;

typedef struct NamedArgExpr
{
    Expr xpr;
    Expr *arg;     /* the argument expression */
    char *name;    /* the name */
    int argnumber; /* argument's number in positional notation */
    int location;  /* argument name location, or -1 if unknown */
} NamedArgExpr;

typedef struct InlineCodeBlock
{
    NodeTag type;
    char *source_text; /* source text of anonymous code block */
    bool atomic;       /* atomic execution context */
} InlineCodeBlock;

#define newNode(size, tag)                                                \
    ({                                                                    \
        Node *_result;                                                    \
        AssertMacro((size) >= sizeof(Node)); /* need the tag, at least */ \
        _result = (Node *)palloc0fast(size);                              \
        _result->type = (tag);                                            \
        _result;                                                          \
    })
#define makeNode(_type_) ((_type_ *)newNode(sizeof(_type_), T_##_type_))
#define nodeTag(nodeptr) (((const Node *)(nodeptr))->type)
#define IsA(nodeptr, _type_) (nodeTag(nodeptr) == T_##_type_)

#define castNode(_type_, nodeptr) ((_type_ *)(nodeptr))

// first used on pl_comp.c
// 参考 pg parse_node.h 92
typedef struct ParseState ParseState;
typedef struct ParamRef
{
    NodeTag type;
    int number;   /* the number of the parameter */
    int location; /* token location, or -1 if unknown */
} ParamRef;

typedef struct ColumnRef
{
    NodeTag type;
    List *fields; /* field names (Value strings) or A_Star */
    int location; /* token location, or -1 if unknown */
} ColumnRef;

typedef enum ParamKind
{
    PARAM_EXTERN,
    PARAM_EXEC,
    PARAM_SUBLINK,
    PARAM_MULTIEXPR
} ParamKind;

typedef struct Param
{
    Expr xpr;
    ParamKind paramkind; /* kind of parameter. See above */
    int paramid;         /* numeric ID for parameter */
    Oid paramtype;       /* pg_type OID of parameter's datatype */
    int32 paramtypmod;   /* typmod value, if known */
    Oid paramcollid;     /* OID of collation, or InvalidOid if none */
    int location;        /* token location, or -1 if unknown */
} Param;

typedef Node *(*PreParseColumnRefHook)(ParseState *pstate, ColumnRef *cref);
typedef Node *(*PostParseColumnRefHook)(ParseState *pstate, ColumnRef *cref, Node *var);
typedef Node *(*ParseParamRefHook)(ParseState *pstate, ParamRef *pref);
typedef Node *(*CoerceParamHook)(ParseState *pstate, Param *param,
                                 Oid targetTypeId, int32 targetTypeMod,
                                 int location);
typedef struct ParseState
{
    PreParseColumnRefHook p_pre_columnref_hook;
    PostParseColumnRefHook p_post_columnref_hook;
    ParseParamRefHook p_paramref_hook;
    //CoerceParamHook p_coerce_param_hook;
    void *p_ref_hook_state; /* common passthrough link for above */
} ParseState;

typedef struct Value
{
    NodeTag type; /* tag appropriately (eg. T_String) */
    union ValUnion {
        int ival;  /* machine integer */
        char *str; /* string */
    } val;
} Value;
#define intVal(v) (((Value *)(v))->val.ival)
#define floatVal(v) atof(((Value *)(v))->val.str)
#define strVal(v) (((Value *)(v))->val.str)

Value *makeString(char *str);

// node related struct
typedef struct ResultSetInfo
{
    NodeTag type;
    ExprContext *econtext;
} ResultSetInfo;

typedef enum
{
    SFRM_ValuePerCall = 0x01,         /* one value returned per call */
    SFRM_Materialize = 0x02,          /* result set instantiated in Tuplestore */
    SFRM_Materialize_Random = 0x04,   /* Tuplestore needs randomAccess */
    SFRM_Materialize_Preferred = 0x08 /* caller prefers Tuplestore */
} SetFunctionReturnMode;

typedef struct ReturnSetInfo
{
    NodeTag type;
    ExprContext *econtext;
    int allowedModes;
    int returnMode;
    void *expectedDesc;
} ReturnSetInfo;

// Param List info struct
typedef struct _ParamListInfo
{
    void *parserSetupArg;

} _ParamListInfo;

typedef struct _ParamListInfo *ParamListInfo;

#define PARAM_FLAG_CONST 0x0001 /* parameter is constant */
typedef struct ParamExternData
{
    Datum value;   /* parameter value */
    bool isnull;   /* is it NULL? */
    uint16 pflags; /* flag bits, see above */
    Oid ptype;     /* parameter's datatype, or 0 */
} ParamExternData;

/*
 * For simplicity in APIs, we also wrap utility statements in PlannedStmt
 * nodes; in such cases, commandType == CMD_UTILITY, the statement itself
 * is in the utilityStmt field, and the rest of the struct is mostly dummy.
 * (We do use canSetTag, stmt_location, stmt_len, and possibly queryId.)
 * ----------------
 */
typedef struct PlannedStmt
{
    NodeTag type;

    uint64 queryId; /* query identifier (copied from Query) */

    bool hasReturning; /* is it insert|update|delete RETURNING? */

    bool hasModifyingCTE; /* has insert|update|delete in WITH? */

    bool canSetTag; /* do I set the command result tag? */

    bool transientPlan; /* redo plan when TransactionXmin changes? */

    bool dependsOnRole; /* is plan specific to current role? */

    bool parallelModeNeeded; /* parallel mode required to execute? */

    int jitFlags; /* which forms of JIT should be performed */

    struct Plan *planTree; /* tree of Plan nodes */

    List *rtable; /* list of RangeTblEntry nodes */

    /* rtable indexes of target relations for INSERT/UPDATE/DELETE */
    List *resultRelations; /* integer list of RT indexes, or NIL */

    /*
    |* rtable indexes of partitioned table roots that are UPDATE/DELETE
    |* targets; needed for trigger firing.
    |*/
    List *rootResultRelations;

    List *subplans; /* Plan trees for SubPlan expressions; note
                                ┊* that some could be NULL */

    Bitmapset *rewindPlanIDs; /* indices of subplans that require REWIND */

    List *rowMarks; /* a list of PlanRowMark's */

    List *relationOids; /* OIDs of relations the plan depends on */

    List *invalItems; /* other dependencies, as PlanInvalItems */

    List *paramExecTypes; /* type OIDs for PARAM_EXEC Params */

    Node *utilityStmt; /* non-null if this is utility stmt */

    /* statement location in source string (copied from Query) */
    int stmt_location; /* start location, or -1 if unknown */
    int stmt_len;      /* length in bytes; 0 means "rest of string" */
} PlannedStmt;

typedef struct Plan
{
    NodeTag type;

    /*
    |* planner's estimate of result size of this plan step
    |*/
    double plan_rows; /* number of rows plan is expected to emit */
    int plan_width;   /* average row width in bytes */

    /*
    |* information needed for parallel query
    |*/
    bool parallel_aware; /* engage parallel-aware logic? */
    bool parallel_safe;  /* OK to use as part of parallel plan? */

    /*
    |* Common structural data for all Plan types.
    |*/
    int plan_node_id;      /* unique across entire final plan tree */
    List *targetlist;      /* target list to be computed at this node */
    List *qual;            /* implicitly-ANDed qual conditions */
    struct Plan *lefttree; /* input plan tree(s) */
    struct Plan *righttree;
    List *initPlan; /* Init Plan nodes (un-correlated expr
                                ┊* subselects) */

    /*
    |* Information for management of parameter-change-driven rescanning
    |*
    |* extParam includes the paramIDs of all external PARAM_EXEC params
    |* affecting this plan node or its children.  setParam params from the
    |* node's initPlans are not included, but their extParams are.
    |*
    |* allParam includes all the extParam paramIDs, plus the IDs of local
    |* params that affect the node (i.e., the setParams of its initplans).
    |* These are _all_ the PARAM_EXEC params that affect this node.
    |*/
    Bitmapset *extParam;
    Bitmapset *allParam;
} Plan;
#endif //PLSQL_ZNBasePARSENODES_H
