#!/bin/bash
./znbase sql --insecure --host=:23457 --execute "SET CLUSTER SETTING sql.distsql.hashjoin_prpd.enabled = false;"
./znbase sql --insecure --host=:23457 --execute "SET CLUSTER SETTING sql.distsql.hashjoin_pnr.enabled = false;"
./znbase sql --insecure --host=:23457 --execute "SET CLUSTER SETTING sql.distsql.hashjoin_bashash.enabled = true;"


