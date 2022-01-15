
/* Size of an EXTERNAL datum that contains a pointer to an expanded object */
#define EXPANDED_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(varatt_expanded))

typedef int16 AttrNumber;

typedef struct AttrDefault
{
	AttrNumber adnum;
	char *adbin; /* nodeToString representation of expr */
} AttrDefault;

typedef struct ConstrCheck
{
	char *ccname;
	char *ccbin; /* nodeToString representation of expr */
	bool ccvalid;
	bool ccnoinherit; /* this is a non-inheritable constraint */
} ConstrCheck;

/* This structure contains constraints of a tuple */
typedef struct TupleConstr
{
	AttrDefault *defval;		 /* array */
	ConstrCheck *check;			 /* array */
	struct AttrMissing *missing; /* missing attributes values, NULL if none */
	uint16 num_defval;
	uint16 num_check;
	bool has_not_null;
	bool has_generated_stored;
} TupleConstr;

// 定义一个伪tupledesc
typedef struct FormData_pg_attribute
{
	// Oid atttypid; //参数的类型id
	// int32 atttypmod;
	// int32 attlen;
	// Oid attcollation;
	// bool attisdropped;
	// char attalign;
	Oid			attrelid;		/* OID of relation containing this attribute */
	NameData	attname;		/* name of attribute */

	/*
	 * atttypid is the OID of the instance in Catalog Class pg_type that
	 * defines the data type of this attribute (e.g. int4).  Information in
	 * that instance is redundant with the attlen, attbyval, and attalign
	 * attributes of this instance, so they had better match or Postgres will
	 * fail.
	 */
	Oid			atttypid;

	/*
	 * attstattarget is the target number of statistics datapoints to collect
	 * during VACUUM ANALYZE of this column.  A zero here indicates that we do
	 * not wish to collect any stats about this column. A "-1" here indicates
	 * that no value has been explicitly set for this column, so ANALYZE
	 * should use the default setting.
	 */
	int32		attstattarget ;

	/*
	 * attlen is a copy of the typlen field from pg_type for this attribute.
	 * See atttypid comments above.
	 */
	int16		attlen;

	/*
	 * attnum is the "attribute number" for the attribute:	A value that
	 * uniquely identifies this attribute within its class. For user
	 * attributes, Attribute numbers are greater than 0 and not greater than
	 * the number of attributes in the class. I.e. if the Class pg_class says
	 * that Class XYZ has 10 attributes, then the user attribute numbers in
	 * Class pg_attribute must be 1-10.
	 *
	 * System attributes have attribute numbers less than 0 that are unique
	 * within the class, but not constrained to any particular range.
	 *
	 * Note that (attnum - 1) is often used as the index to an array.
	 */
	int16		attnum;

	/*
	 * attndims is the declared number of dimensions, if an array type,
	 * otherwise zero.
	 */
	int32		attndims;

	/*
	 * fastgetattr() uses attcacheoff to cache byte offsets of attributes in
	 * heap tuples.  The value actually stored in pg_attribute (-1) indicates
	 * no cached value.  But when we copy these tuples into a tuple
	 * descriptor, we may then update attcacheoff in the copies. This speeds
	 * up the attribute walking process.
	 */
	int32		attcacheoff ;

	/*
	 * atttypmod records type-specific data supplied at table creation time
	 * (for example, the max length of a varchar field).  It is passed to
	 * type-specific input and output functions as the third argument. The
	 * value will generally be -1 for types that do not need typmod.
	 */
	int32		atttypmod ;

	/*
	 * attbyval is a copy of the typbyval field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
	bool		attbyval;

	/*----------
	 * attstorage tells for VARLENA attributes, what the heap access
	 * methods can do to it if a given tuple doesn't fit into a page.
	 * Possible values are
	 *		'p': Value must be stored plain always
	 *		'e': Value can be stored in "secondary" relation (if relation
	 *			 has one, see pg_class.reltoastrelid)
	 *		'm': Value can be stored compressed inline
	 *		'x': Value can be stored compressed inline or in "secondary"
	 * Note that 'm' fields can also be moved out to secondary storage,
	 * but only as a last resort ('e' and 'x' fields are moved first).
	 *----------
	 */
	char		attstorage;

	/*
	 * attalign is a copy of the typalign field from pg_type for this
	 * attribute.  See atttypid comments above.
	 */
	char		attalign;

	/* This flag represents the "NOT NULL" constraint */
	bool		attnotnull;

	/* Has DEFAULT value or not */
	bool		atthasdef ;

	/* Has a missing value or not */
	bool		atthasmissing ;

	/* One of the ATTRIBUTE_IDENTITY_* constants below, or '\0' */
	char		attidentity ;

	/* One of the ATTRIBUTE_GENERATED_* constants below, or '\0' */
	char		attgenerated;

	/* Is dropped (ie, logically invisible) or not */
	bool		attisdropped ;

	/*
	 * This flag specifies whether this column has ever had a local
	 * definition.  It is set for normal non-inherited columns, but also for
	 * columns that are inherited from parents if also explicitly listed in
	 * CREATE TABLE INHERITS.  It is also set when inheritance is removed from
	 * a table with ALTER TABLE NO INHERIT.  If the flag is set, the column is
	 * not dropped by a parent's DROP COLUMN even if this causes the column's
	 * attinhcount to become zero.
	 */
	bool		attislocal ;

	/* Number of times inherited from direct parent relation(s) */
	signed int		attinhcount;

	/* attribute's collation */
	unsigned int		attcollation;

} FormData_pg_attribute;

typedef struct TupleDescData
{
	int natts;			 /* 元组中的属性个数*/
	Oid tdtypeid;		 /* 元祖的复合id   */
	int32 tdtypmod;		 /* typmod for tuple type */
	int tdrefcount;		 /* 引用计数 */
	TupleConstr *constr; /* 约束条件，没有的话为空*/
						 // 添加合适的atrribute 对象
	bool rocksDB_get;
	/* 参数中的描述符，attr[N]是第N+1个参数*/
	FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];
} TupleDescData;
typedef struct TupleDescData *TupleDesc;

/*
 * Every expanded object must contain this header; typically the header
 * is embedded in some larger struct that adds type-specific fields.
 *
 * It is presumed that the header object and all subsidiary data are stored
 * in eoh_context, so that the object can be freed by deleting that context,
 * or its storage lifespan can be altered by reparenting the context.
 * (In principle the object could own additional resources, such as malloc'd
 * storage, and use a memory context reset callback to free them upon reset or
 * deletion of eoh_context.)
 *
 * We set up two TOAST pointers within the standard header, one read-write
 * and one read-only.  This allows functions to return either kind of pointer
 * without making an additional allocation, and in particular without worrying
 * whether a separately palloc'd object would have sufficient lifespan.
 * But note that these pointers are just a convenience; a pointer object
 * appearing somewhere else would still be legal.
 *
 * The typedef declaration for this appears in postgres.h.
 */
struct ExpandedObjectHeader
{
	/* Phony varlena header */
	int32 vl_len_; /* always EOH_HEADER_MAGIC, see below */

	/* Pointer to methods required for object type */
	//const ExpandedObjectMethods *eoh_methods;

	/* Memory context containing this header and subsidiary data */
	MemoryContext eoh_context;

	/* Standard R/W TOAST pointer for this object is kept here */
	char eoh_rw_ptr[EXPANDED_POINTER_SIZE];

	/* Standard R/O TOAST pointer for this object is kept here */
	char eoh_ro_ptr[EXPANDED_POINTER_SIZE];
};

typedef struct ExpandedRecordHeader
{
	/* Standard header for expanded objects */
	ExpandedObjectHeader hdr;

	/* Magic value identifying an expanded record (for debugging only) */
	int er_magic;

	/* Assorted flag bits */
	int flags;
#define ER_FLAG_FVALUE_VALID 0x0001	   /* fvalue is up to date? */
#define ER_FLAG_FVALUE_ALLOCED 0x0002  /* fvalue is local storage? */
#define ER_FLAG_DVALUES_VALID 0x0004   /* dvalues/dnulls are up to date? */
#define ER_FLAG_DVALUES_ALLOCED 0x0008 /* any field values local storage? */
#define ER_FLAG_HAVE_EXTERNAL 0x0010   /* any field values are external? */
#define ER_FLAG_TUPDESC_ALLOCED 0x0020 /* tupdesc is local storage? */
#define ER_FLAG_IS_DOMAIN 0x0040	   /* er_decltypeid is domain? */
#define ER_FLAG_IS_DUMMY 0x0080		   /* this header is dummy (see below) */
/* flag bits that are not to be cleared when replacing tuple data: */
#define ER_FLAGS_NON_DATA \
	(ER_FLAG_TUPDESC_ALLOCED | ER_FLAG_IS_DOMAIN | ER_FLAG_IS_DUMMY)

	/* Declared type of the record variable (could be a domain type) */
	Oid er_decltypeid;

	/*
	 * Actual composite type/typmod; never a domain (if ER_FLAG_IS_DOMAIN,
	 * these identify the composite base type).  These will match
	 * er_tupdesc->tdtypeid/tdtypmod, as well as the header fields of
	 * composite datums made from or stored in this expanded record.
	 */
	Oid er_typeid;	 /* type OID of the composite type */
	int32 er_typmod; /* typmod of the composite type */

	/*
	 * Tuple descriptor, if we have one, else NULL.  This may point to a
	 * reference-counted tupdesc originally belonging to the typcache, in
	 * which case we use a memory context reset callback to release the
	 * refcount.  It can also be locally allocated in this object's private
	 * context (in which case ER_FLAG_TUPDESC_ALLOCED is set).
	 */
	TupleDesc er_tupdesc;

	/*
	 * Unique-within-process identifier for the tupdesc (see typcache.h). This
	 * field will never be equal to INVALID_TUPLEDESC_IDENTIFIER.
	 */
	uint64 er_tupdesc_id;

	/*
	 * If we have a Datum-array representation of the record, it's kept here;
	 * else ER_FLAG_DVALUES_VALID is not set, and dvalues/dnulls may be NULL
	 * if they've not yet been allocated.  If allocated, the dvalues and
	 * dnulls arrays are palloc'd within the object private context, and are
	 * of length matching er_tupdesc->natts.  For pass-by-ref field types,
	 * dvalues entries might point either into the fstartptr..fendptr area, or
	 * to separately palloc'd chunks.
	 */
	Datum *dvalues; /* array of Datums */
	bool *dnulls;	/* array of is-null flags for Datums */
	int nfields;	/* length of above arrays */

	/*
	 * flat_size is the current space requirement for the flat equivalent of
	 * the expanded record, if known; otherwise it's 0.  We store this to make
	 * consecutive calls of get_flat_size cheap.  If flat_size is not 0, the
	 * component values data_len, hoff, and hasnull must be valid too.
	 */
	Size flat_size;

	Size data_len; /* data len within flat_size */
	int hoff;	   /* header offset */
	bool hasnull;  /* null bitmap needed? */

	/*
	 * fvalue points to the flat representation if we have one, else it is
	 * NULL.  If the flat representation is valid (up to date) then
	 * ER_FLAG_FVALUE_VALID is set.  Even if we've outdated the flat
	 * representation due to changes of user fields, it can still be used to
	 * fetch system column values.  If we have a flat representation then
	 * fstartptr/fendptr point to the start and end+1 of its data area; this
	 * is so that we can tell which Datum pointers point into the flat
	 * representation rather than being pointers to separately palloc'd data.
	 */
	//HeapTuple	fvalue;			[> might or might not be private storage <]
	char *fstartptr; /* start of its data area */
	char *fendptr;	 /* end+1 of its data area */

	/* Some operations on the expanded record need a short-lived context */
	MemoryContext er_short_term_cxt; /* short-term memory context */

	/* Working state for domain checking, used if ER_FLAG_IS_DOMAIN is set */
	struct ExpandedRecordHeader *er_dummy_header; /* dummy record header */
	void *er_domaininfo;						  /* cache space for domain_check() */

	/* Callback info (it's active if er_mcb.arg is not NULL) */
	MemoryContextCallback er_mcb;
} ExpandedRecordHeader;

// 各个类型的成员默认值具体需要参照pg, 此处只是为了通过编译所定义的结构
typedef struct HeapTupleData
{
	uint32 t_len;	 /* length of *t_data */
	int64_t t_rowid; /* rowid array */
	Datum *values;
	void *t_data; /* -> tuple header and data */
} HeapTupleData;

typedef struct _HeapTupleHeader
{

} _HeapTupleHeader;
typedef struct _HeapTupleHeader *HeapTupleHeader;

typedef struct Relation
{

} Relation;

typedef struct TupleConversionMap
{
	TupleDesc indesc;	 /* tupdesc for source rowtype */
	TupleDesc outdesc;	 /* tupdesc for result rowtype */
	AttrNumber *attrMap; /* indexes of input fields, or 0 for null */
	Datum *invalues;	 /* workspace for deconstructing source */
	bool *inisnull;
	Datum *outvalues; /* workspace for constructing result */
	bool *outisnull;
} TupleConversionMap;

typedef struct FormData_pg_proc
{
	char *proc_source;
	NameData proname;
	Oid argtypes[255];
	char **argTypeNames;
	char **argnames;
	char *argModes;
	Oid prorettype;
	uint32 pronargs;
	char prokind;
	bool proretset;
	char provolatile;
	
	
	char *file_path;

	char *file_name;
	char *file_name_full;
	char *language;
	oidvector proargtypes;
} FormData_pg_proc;

typedef struct FormData_pg_class
{
	char relkind;
} FormData_pg_class;

typedef HeapTupleData *HeapTuple;
typedef FormData_pg_proc *Form_pg_proc;
typedef FormData_pg_class *Form_pg_class;
typedef FormData_pg_attribute *Form_pg_attribute;

#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])

// 函数声明
char *format_type_be(Oid type_oid); //通过type_oid 得到 type_name 用于报错使用

typedef struct Trigger
{
	Oid tgoid; /* OID of trigger (pg_trigger row) */
	/* Remaining fields are copied from pg_trigger, see pg_trigger.h */
	char *tgname;
	Oid tgfoid;
	int16 tgtype;
	char tgenabled;
	bool tgisinternal;
	Oid tgconstrrelid;
	Oid tgconstrindid;
	Oid tgconstraint;
	bool tgdeferrable;
	bool tginitdeferred;
	int16 tgnargs;
	int16 tgnattr;
	int16 *tgattr;
	char **tgargs;
	char *tgqual;
	char *tgoldtable;
	char *tgnewtable;
} Trigger;

typedef struct TriggerData
{
	NodeTag type;
	Trigger *tg_trigger;
	int32 tg_event;
	HeapTuple tg_trigtuple;
	HeapTuple tg_newtuple;
} TriggerData;

typedef struct EventTriggerData
{
	NodeTag type;
} EventTriggerData;

#define GETSTRUCT(TUP) NULL
