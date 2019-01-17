#!/usr/bin/env bash
hadoop fs -mkdir holmes-input
hadoop fs -put holmes/input/* holmes-input/
hadoop jar wordcount-1.0.jar WordCount holmes-input holmes-output
mkdir -p holmes/output
hadoop fs -get holmes-output holmes/output
tar -cvzf output.tar.gz -C holmes/output/ .
mv output.tar.gz holmes
rm -rf holmes/output/
