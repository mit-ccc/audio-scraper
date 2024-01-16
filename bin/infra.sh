#!/bin/bash

set -Eeuo pipefail

aws cloudformation create-stack \
    --stack-name "$APPLICATION_NAME" \
    --capabilities CAPABILITY_NAMED_IAM \
    \
    --template-body infra.yml \
    \
    --parameters \
        ParameterKey=POSTGRES_USER,ParameterValue="$POSTGRES_USER" \
        ParameterKey=POSTGRES_DB,ParameterValue="$POSTGRES_DB" \
        ParameterKey=PGPORT,ParameterValue="$PGPORT" \
        \
        ParameterKey=TargetS3Bucket,ParameterValue="$S3_BUCKET" \
        ParameterKey=TargetS3Prefix,ParameterValue="$S3_PREFIX" \
        \
        ParameterKey=LOG_LEVEL,ParameterValue="$LOG_LEVEL" \
        ParameterKey=N_TASKS,ParameterValue="$N_TASKS" \
        ParameterKey=POLL_INTERVAL,ParameterValue="$POLL_INTERVAL" \
        ParameterKey=CHUNK_SIZE,ParameterValue="$CHUNK_SIZE" \
        ParameterKey=CHUNK_ERROR_BEHAVIOR,ParameterValue="$CHUNK_ERROR_BEHAVIOR" \
        ParameterKey=CHUNK_ERROR_THRESHOLD,ParameterValue="$CHUNK_ERROR_THRESHOLD" \
        \
        ParameterKey=ApplicationName,ParameterValue="$APPLICATION_NAME" \
        ParameterKey=DesiredWorkerCount,ParameterValue="$WORKER_COUNT" \
        ParameterKey=Vpc,ParameterValue="$VPC" \
        ParameterKey=SubnetA,ParameterValue="$SUBNETA" \
        ParameterKey=SubnetB,ParameterValue="$SUBNETB" \
