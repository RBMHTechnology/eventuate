#!/bin/bash

if [[ $1 = 2.11.* ]]; then
    sbt ++$1 publish adapterSpark/publish exampleSpark/publish ghpagesPushSite
else
    sbt ++$1 publish
fi
