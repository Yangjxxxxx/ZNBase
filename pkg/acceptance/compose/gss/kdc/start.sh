#!/bin/sh

set -e

# The /keytab directory is volume mounted on both kdc and znbase. kdc
# can create the keytab with kadmin.local here and it is then useable
# by znbase.
kadmin.local -q "ktadd -k /keytab/znbase.keytab postgres/gss_znbase_1.gss_default@MY.EX"

krb5kdc -n
