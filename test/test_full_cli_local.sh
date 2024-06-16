#!/bin/bash
set -eu
set -o pipefail

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

SCRIPT_DIR="$(dirname "$0")"

docker run --rm -p 9000:9000 -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minio123 minio/minio minio server /data > minio-server.log &
# -v data:/data
# --name=minio --health-cmd "curl http://localhost:9000/minio/health/live"

# env AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minio123 aws --endpoint-url http://127.0.0.1:9000/ s3 ls

export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minio123"
export AWS_DEFAULT_REGION="eu-west-2" # Unused, but needed: https://stackoverflow.com/a/68348234/118608

# Create bucket
aws --endpoint-url http://127.0.0.1:9000/ s3 mb s3://minio-test

# Create file with random content
dd bs=1 count=1000 if=/dev/random > "./random-file.before" 2>/dev/null
aws --endpoint-url http://127.0.0.1:9000/ s3 cp random-file.before s3://minio-test/random-file.after
aws --endpoint-url http://127.0.0.1:9000/ s3 cp s3://minio-test/random-file.after random-file.after
shasum random-file.before random-file.after


"$SCRIPT_DIR/test_full_cli.sh"
