#!/bin/bash -exv

cd code && zip --update -r ../thingiverse.zip || true
cd ..
pwd
AWS_PROFILE=pjnet aws s3 cp thingiverse.zip s3://tedder-us-east-1/lambda/thingiverse.zip
AWS_PROFILE=pjnet aws --region us-east-1 lambda update-function-code --function-name thingiverse_bow --s3-bucket tedder-us-east-1 --s3-key lambda/thingiverse.zip --publish

