#!/bin/sh

sbt $1 "adapterSpark/it:testOnly com.rbmhtechnology.eventuate.adapter.spark.SparkStreamAdapterSpec"
