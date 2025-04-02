#!/bin/bash

QUOTIENT_BITS=$1
REMAINDER_BITS=$2
NUM_QUERIES=$3

make clean
make test_throughput
./test_throughput ${QUOTIENT_BITS}  ${REMAINDER_BITS} ${NUM_QUERIES} > output.txt
