#!/bin/bash
cat $1 | xargs -P 0 -d '\n' -I ARGS bash -c 'a="ARGS"; args=($a); echo "${args[@]:2}" | nc -4u -w0 -q1 ${args[@]:0:2}'
# For testing the command
#cat $1 | xargs -P 0 -td '\n' -I ARGS bash -xc 'a="ARGS"; args=($a); echo "${args[@]:2}" | nc -4u -w0 -q1 ${args[@]:0:2}'

# Populate a text file with something like:
# localhost 6855 LTC getblocktemplate signal=1 __spawn=1
# localhost 6856 LTC getblocktemplate signal=1 __spawn=1
# localhost 6857 LTC getblocktemplate signal=1 __spawn=1
# To do push block notification for each of them. File changes take effect instantly

