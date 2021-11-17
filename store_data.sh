#!/bin/bash

bucket=$(cat /tmp/bucket)
s3cli put $bucket /image.png
echo File /image.png stored into $bucket
