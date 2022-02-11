#!/bin/bash
set -euxo pipefail 


REGION="eu-west-1"
ACCOUNT_ID="338791806049" 


aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ACCOUNT_ID}".dkr.ecr."${REGION}".amazonaws.com 
docker tag 9f84f154a4cd "${ACCOUNT_ID}".dkr.ecr."${REGION}".amazonaws.com/anas_repo:academy-capstone-winter-2022 
docker push "${ACCOUNT_ID}".dkr.ecr."${REGION}".amazonaws.com/anas_repo:academy-capstone-winter-2022