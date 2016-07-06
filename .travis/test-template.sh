#!/bin/bash

sbt $1 "$2/test:testOnly";  exit_1=$?
sbt $1 "$2/it:testOnly";    exit_2=$?
sbt $1 "$2/multi-jvm:test"; exit_3=$?

! (( $exit_1 || $exit_2 || $exit_3 ))
