#!/bin/bash

set -eux

echo ">>> Copying tables to Minio"

echo ">>> Creating Minio store"
mc config host add store http://minio:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

echo ">>> Creating bucket"
mc mb store/$MINIO_TEST_BUCKET

echo ">>> Unzipping tables"
find /opt/data -name "*.zip" -exec unzip {} -d /tmp/data/ \;

echo ">>> Copying tables to Minio"
find /tmp/data -type d -mindepth 1 -maxdepth 1 -exec mc cp --recursive {} store/$MINIO_TEST_BUCKET \;

echo ">>> Listing uploaded tables"
mc ls store/$MINIO_TEST_BUCKET
