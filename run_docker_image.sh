#!/bin/sh


ACCESS_KEY_ID=$(cat ~/.aws/credentials | grep aws_access_key | cut -d "=" -f 2 | awk '{$1=$1};1')
SECRET_ACCESS_KEY=$(cat ~/.aws/credentials | grep aws_secret_access_key | cut -d "=" -f 2 | awk '{$1=$1};1')
REGION="eu-west-1"
DEFAULT_REGION="eu-west-1"

# --env AWS_REGION=$REGION
# --env AWS_DEFAULT_REGION=$DEFAULT_REGION

docker run -it \
--env AWS_DEFAULT_REGION="${DEFAULT_REGION}" \
--env AWS_REGION="${REGION}" \
--env AWS_ACCESS_KEY_ID="${ACCESS_KEY_ID}" \
--env AWS_SECRET_ACCESS_KEY="${SECRET_ACCESS_KEY}" \
environment:academy-capstone-winter-2022