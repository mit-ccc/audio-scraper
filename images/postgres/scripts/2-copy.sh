#!/bin/bash

set -Eeuo pipefail

psql <<"EOF"
COPY data.source
FROM '/usr/src/app/source-data.csv'
WITH (
    FORMAT csv,
    HEADER MATCH,
    DELIMITER E'\t'
);
EOF
