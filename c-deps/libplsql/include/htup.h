//
// Created by chenmingsong on 2020/5/13.
//

#ifndef PLSQL_HTUP_H
#define PLSQL_HTUP_H
#define GETSTRUCT(TUP) NULL
// 各个类型的成员默认值具体需要参照pg, 此处只是为了通过编译所定义的结构
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
} TriggerData;

typedef struct EventTriggerData
{
    NodeTag type;
} EventTriggerData;

typedef struct HeapTupleData
{
    uint32 t_len;    /* length of *t_data */
    int64_t t_rowid; /* rowid array */
    Datum *values;
    void *t_data; /* -> tuple header and data */
} HeapTupleData;

typedef struct FormData_pg_proc
{
    // TODO: add comment
    char *proc_source;
    NameData proname;
    Oid prorettype;
    int16 pronargs;
    char prokind;
    bool proretset;
    char provolatile;
    oidvector proargtypes;
} FormData_pg_proc;

typedef struct FormData_pg_type
{
    Oid oid;
    NameData typname;
    int16 typlen;
    char typtype;
    bool typbyval;
    bool typisdefined;
    Oid typrelid;
    Oid typbasetype;
    Oid typcollation;
    Oid typelem;
    char typstorage;
} FormData_pg_type;

typedef struct FormData_pg_class
{
    char relkind;
} FormData_pg_class;

typedef struct FormData_pg_attribute
{
    Oid atttypid;
    int32 atttypmod;
    Oid attcollation;
} FormData_pg_attribute;

typedef HeapTupleData *HeapTuple;
typedef FormData_pg_proc *Form_pg_proc;
typedef FormData_pg_type *Form_pg_type;
typedef FormData_pg_class *Form_pg_class;
typedef FormData_pg_attribute *Form_pg_attribute;

// 函数声明
char *format_type_be(Oid type_oid); //通过type_oid 得到 type_name 用于报错使用

#endif //PLSQL_HTUP_H
