#!/usr/bin/env bash
hadoop fs -put input/buys.txt .
hadoop fs -put input/clicks.txt .
hadoop jar partc-successrate-1.0.jar SuccessRate buys.txt clicks.txt buy-count/ clicks-count/ join-count/ topk-success-rate-out/
mkdir output
hadoop fs -get topk-success-rate-out/* output
mv output/*part* output.txt
rm -rf output