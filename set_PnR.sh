#!/bin/bash
./znbase sql --insecure --host=:23457 --execute "SET CLUSTER SETTING sql.distsql.hashjoin_prpd.enabled = false;"
./znbase sql --insecure --host=:23457 --execute "SET CLUSTER SETTING sql.distsql.hashjoin_pnr.enabled = true;"
./znbase sql --insecure --host=:23457 --execute "SET CLUSTER SETTING sql.distsql.hashjoin_bashash.enabled = false;"


