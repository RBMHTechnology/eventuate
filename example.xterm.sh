#!/bin/bash

for site in A B C D E F
do
    xterm -title $site -e "./example-site $site $*" &
done
