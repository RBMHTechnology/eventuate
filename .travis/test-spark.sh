#!/bin/sh

if [[ $1 = 2.11.* ]]; then
    sbt ++$1 "adapterSpark/it:testOnly com.rbmhtechnology.eventuate.adapter.spark.SparkStreamAdapterSpec"
fi
