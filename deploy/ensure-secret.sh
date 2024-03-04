#!/bin/bash

set -Eeuo pipefail

secret_name="$APPLICATION_NAME-postgres-password"
secret_value="$POSTGRES_PASSWORD"

if aws secretsmanager describe-secret --secret-id "$secret_name" > /dev/null 2>&1; then
    aws secretsmanager update-secret \
        --secret-id "$secret_name" \
        --secret-string "$secret_value"
else
    aws secretsmanager create-secret \
        --name "$secret_name" \
        --secret-string "$secret_value"
fi
