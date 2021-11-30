#!/bin/bash
pip freeze > requirements.txt
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 555050780997.dkr.ecr.ap-southeast-2.amazonaws.com
docker build -t oa-process-pricing-from-api .
docker tag oa-process-pricing-from-api:latest 555050780997.dkr.ecr.ap-southeast-2.amazonaws.com/oa-process-pricing-from-api:latest
docker push 555050780997.dkr.ecr.ap-southeast-2.amazonaws.com/oa-process-pricing-from-api:latest