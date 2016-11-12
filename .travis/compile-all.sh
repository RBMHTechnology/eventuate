#!/bin/bash

if [[ $1 = 2.11.* ]]; then
    sbt ++$1 test:compile adapterSpark/test:compile exampleSpark/test:compile
else
    sbt ++$1 test:compile
fi
