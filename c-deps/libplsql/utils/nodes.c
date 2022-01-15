#include "plpgsql.h"
Value *
makeString(char *str)
{
    Value      *v = makeNode(Value);

    v->type = T_String;
    v->val.str = str;
    return v;
}

