#!/usr/bin/env bash
hadoop fs -mkdir imdb-input
hadoop fs -put imdb/input/* imdb-input/
hadoop jar wordcount-1.0.jar WordCount imdb-input imdb-output
mkdir -p imdb/output
hadoop fs -get imdb-output imdb/output
tar -cvzf output.tar.gz -C imdb/output/ .
mv output.tar.gz imdb
rm -rf imdb/output/
