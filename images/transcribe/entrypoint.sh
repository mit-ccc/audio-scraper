#!/bin/bash
# Variables are assumed to be in the environment

set -Eeuo pipefail

wait-for-it "${POSTGRES_HOST:-postgres}:${PGPORT:-5432}" -- echo "Postgres up"

cat > ~/.odbc.ini << EOF
[Database]
Driver = /usr/lib/$(uname -m)-linux-gnu/odbc/psqlodbcw.so
Servername = ${POSTGRES_HOST:-postgres}
Port = ${PGPORT:-5432}
Database = ${POSTGRES_DB:-postgres}
UserName = ${POSTGRES_USER:-postgres}
Password = ${POSTGRES_PASSWORD}
BoolsAsChar = 0
EOF

exec "$@"
