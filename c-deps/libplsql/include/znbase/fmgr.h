// 函数管理
typedef struct Node *fmNodePtr;
typedef struct FmgrInfo
{
	Oid fn_oid;			   /* OID of function (NOT of handler, if any) */
	MemoryContext fn_mcxt; /* memory context to store fn_extra in */
	void *fn_extra;		   /* extra space for use by handler */
	fmNodePtr fn_expr;	   /* expression parse tree for call, or NULL */
	Oid argtypes[255];
	char *argnames;
} FmgrInfo;

typedef struct FunctionCallInfoBaseData
{
    // TODO: 属性改名
	FmgrInfo *flinfo;
	FmgrInfo sflinfo;
	char *hash_of_sql;
	fmNodePtr resultinfo;
	fmNodePtr context;
	bool isnull;
	short nargs;
	Oid fncollation; /* collation for function to use */
#define FIELDNO_FUNCTIONCALLINFODATA_ARGS 6
	NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
} FunctionCallInfoBaseData;

typedef struct FunctionCallInfoBaseDataForGO
{
	FmgrInfo *flinfo;
	FmgrInfo sflinfo;
	char *hash_of_sql;
	fmNodePtr resultinfo;
	fmNodePtr context;
	bool isnull;
	short nargs;
	Oid fncollation; /* collation for function to use */
	#define FIELDNO_FUNCTIONCALLINFODATA_ARGS 6
}FunctionCallInfoBaseDataForGO;


typedef struct FunctionCallInfoBaseData *FunctionCallInfo;
#define PG_FUNCTION_ARGS FunctionCallInfo fcinfo

/*
 * Space needed for a FunctionCallInfoBaseData struct with sufficient space
 * for `nargs` arguments.
 */
#define SizeForFunctionCallInfo(nargs)          \
	(offsetof(FunctionCallInfoBaseData, args) + \
	 sizeof(NullableDatum) * (nargs))

/*
 * This macro ensures that `name` points to a stack-allocated
 * FunctionCallInfoBaseData struct with sufficient space for `nargs` arguments.
 */
#define LOCAL_FCINFO(name, nargs)                                        \
	/* use union with FunctionCallInfoBaseData to guarantee alignment */ \
	union {                                                              \
		FunctionCallInfoBaseData fcinfo;                                 \
		/* ensure enough space for nargs args is available */            \
		char fcinfo_data[SizeForFunctionCallInfo(nargs)];                \
	} name##data;                                                        \
	FunctionCallInfo name = &name##data.fcinfo

#define PG_GETARG_DATUM(n) (fcinfo->args[n].value)
#define PG_GETARG_OID(n) DatumGetObjectId(PG_GETARG_DATUM(n))
