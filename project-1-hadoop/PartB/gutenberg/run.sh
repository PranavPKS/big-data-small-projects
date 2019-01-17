#!/usr/bin/env bash
hadoop jar wordcount-1.0.jar WordCountTopK s3://csci5253-gutenberg-dataset/ s3://csci5253-fall18-p1-bucket/gutenberg-temp s3://csci5253-fall18-p1-bucket/gutenberg-topk
aws s3 cp s3://csci5253-fall18-p1-bucket/gutenberg-topk gutenberg/output --recursive
tar -cvzf output.tar.gz -C gutenberg/output/ .
mv output.tar.gz gutenberg
rm -rf gutenberg/output/
