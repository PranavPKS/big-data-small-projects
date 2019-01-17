#!/usr/bin/env bash
hadoop jar wordcount-1.0.jar WordCountTopK s3://wordcount-datasets/ s3://project-5253-bucket-final/yelp-temp s3://project-5253-bucket-final/yelp-topk
aws s3 cp s3://project-5253-bucket-final/yelp-topk yelp/output --recursive
tar -cvzf output.tar.gz -C yelp/output/ .
mv output.tar.gz yelp
rm -rf yelp/output/
