#!/bin/sh

sbt $1 "adapterStream/test:testOnly";  exit_1=$?
sbt $1 "adapterStream/it:testOnly";    exit_2=$?

! (( $exit_1 || $exit_2))
