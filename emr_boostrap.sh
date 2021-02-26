#!/bin/bash
set -euxo pipefail
sudo pip3 install awscli boto boto3 sqlalchemy
mkdir -p /home/hadoop/pyspark_code
aws s3 cp s3://sparkify-pedre-datalake/pyspark_code/code/ /home/hadoop/pyspark_code/ --recursive --include "*"