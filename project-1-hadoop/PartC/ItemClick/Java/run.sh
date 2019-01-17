#!/usr/bin/env bash
hadoop fs -put input/clicks.txt .
hadoop jar partc-itemclick-1.0.jar ItemClick clicks.txt clicks-temp topk-items-out
mkdir output
hadoop fs -get topk-items-out/* output
mv output/*part* output.txt
rm -rf output
