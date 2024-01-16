#!/bin/bash

set -Eeuo pipefail

psql <<"EOF"
COPY data.station
FROM '/usr/src/app/station-data.csv'
WITH (
    FORMAT csv,
    HEADER MATCH,
    DELIMITER E'\t'
);
EOF
