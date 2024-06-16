#!/bin/bash
set -eu
set -o pipefail

mkdir -p files-random
mkdir -p files-random-original

# Create 100 files of varying sizes (1-1000 bytes)
for i in $(seq 100); do
  size=$((1 + $RANDOM % 1000))
  dd bs=1 count=$size if=/dev/random > "./files-random/$i" 2>/dev/null
done
cp -Rf ./files-random/* ./files-random-original

diff -r ./files-random ./files-random-original

# python cli.py build
# python cli.py upload-all
# python cli.py download-all
# python cli.py extract-all

export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minio123"
export AWS_DEFAULT_REGION="eu-west-2" # Unused, but needed: https://stackoverflow.com/a/68348234/118608
aws --endpoint-url http://127.0.0.1:9000/ s3 mb s3://bitum-bucket || true

set -x

# Upload files
python bitum/cli.py upload --create --endpoint-url http://127.0.0.1:9000/ --bucket bitum-bucket files-random # python cli.py sync

# Delete files
/bin/rm -rf files-random/
mkdir -p files-random/

# Download files
python bitum/cli.py download --endpoint-url http://127.0.0.1:9000/ --bucket bitum-bucket files-random # python cli.py sync

# Check that the downloaded files are correct
diff -r ./files-random ./files-random-original

/bin/rm -rf files-random/ files-random-original/
