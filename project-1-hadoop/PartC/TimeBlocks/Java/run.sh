#!/usr/bin/env bash
hadoop fs -put input/buys.txt .
hadoop jar partc-timeblocks-1.0.jar TimeBlocks buys.txt time-blocks-temp time-blocks-out
mkdir output
hadoop fs -get time-blocks-out/* output
mv output/*part* output.txt
rm -rf output
