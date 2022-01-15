// Expr 将会根据znbase 内的表达式进行适配

typedef struct Expr
{
	NodeTag		type;
} Expr;

typedef struct ExprContext {
    void *ecxt_per_tuple_memory;

} ExprContext;

typedef struct ExprState
{
	NodeTag		tag;

	uint8		flags;			/* bitmask of EEO_FLAG_* bits, see above */

	/*
	 * Storage for result value of a scalar expression, or for individual
	 * column results within expressions built by ExecBuildProjectionInfo().
	 */
#define FIELDNO_EXPRSTATE_RESNULL 2
	bool		resnull;
#define FIELDNO_EXPRSTATE_RESVALUE 3
	Datum		resvalue;

	/*
	 * If projecting a tuple result, this slot holds the result; else NULL.
	 */
#define FIELDNO_EXPRSTATE_RESULTSLOT 4
    // znbase comment
    // TupleTableSlot 必须重新设计
	//TupleTableSlot *resultslot;

    // znbase comment
    // Expr 表达式求值， 需要重新结合znbase 求值过程设计
	/*
	 * Instructions to compute expression's return value.
	 */
	//struct ExprEvalStep *steps;

	/*
	 * Function that actually evaluates the expression.  This can be set to
	 * different values depending on the complexity of the expression.
	 */
	//ExprStateEvalFunc evalfunc;

	/* original expression tree, for debugging only */
	Expr	   *expr;

	/* private state for an evalfunc */
	void	   *evalfunc_private;

	/*
	 * XXX: following fields only needed during "compilation" (ExecInitExpr);
	 * could be thrown away afterwards.
	 */

	int			steps_len;		/* number of steps currently */
	int			steps_alloc;	/* allocated length of steps array */
    // znbase comment
    // 暂时注释掉， 可能PLSQL 里面未引用 相关成员
	//struct PlanState *parent;	/* parent PlanState node, if any */
	//ParamListInfo ext_params;	/* for compiling PARAM_EXTERN nodes */

	Datum	   *innermost_caseval;
	bool	   *innermost_casenull;

	Datum	   *innermost_domainval;
	bool	   *innermost_domainnull;
} ExprState;

typedef struct CachedExpression {
    Expr *expr;
    bool is_valid;
} CachedExpression;

typedef struct FuncExpr {
    Expr        xpr;
    Oid         funcid;         /* PG_PROC OID of the function */
    Oid         funcresulttype; /* PG_TYPE OID of result value */
    bool        funcretset;     /* true if function returns set */
    bool        funcvariadic;   /* true if variadic arguments have been
                                ┊* combined into an array last argument */
    Oid         funccollid;     /* OID of collation of result */
    Oid         inputcollid;    /* OID of collation that function should use */
    List       *args;           /* arguments to the function */
    int         location;       /* token location, or -1 if unknown */
} FuncExpr;

CachedExpression *GetCachedExpression(Expr *expr);
Datum ExecEvalExpr(Expr *expr, ExprContext *context, bool *isNull);




