#!/bin/bash
./znbase start --insecure --store=./node1 --listen-addr=:23457 --http-addr=:8001 --join=:23457,:23458,:23459 --background
./znbase start --insecure --store=./node2 --listen-addr=:23458 --http-addr=:8002 --join=:23457,:23458,:23459 --background
./znbase start --insecure --store=./node3 --listen-addr=:23459 --http-addr=:8003 --join=:23457,:23458,:23459 --background

