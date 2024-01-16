#!/bin/bash

set -Eeuo pipefail

# Variables are assumed to be in the environment
cat > ~/.odbc.ini << EOF
[Database]
Driver = /usr/lib/$(uname -m)-linux-gnu/odbc/psqlodbcw.so
Servername = $POSTGRES_HOST
Port = $PGPORT
Database = $POSTGRES_DB
UserName = $POSTGRES_USER
Password = $POSTGRES_PASSWORD
BoolsAsChar = 0
EOF

exec "$@"
