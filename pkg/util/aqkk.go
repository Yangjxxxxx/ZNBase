package util

import "github.com/znbasedb/znbase/pkg/util/envutil"

// EnableAQKK is used for aqkk 编译控制
var EnableAQKK = envutil.EnvOrDefaultBool("ZNBASE_AQKK", false)

// EnableUDR is used for UDR 编译控制
var EnableUDR = envutil.EnvOrDefaultBool("ZNBASE_UDR", false)
