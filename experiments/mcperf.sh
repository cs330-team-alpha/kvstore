#/bin/bash

mcperf --linger=0 --timeout=60 --conn-rate=1000 --call-rate=1000 --num-calls=100 --num-conns=1000 --sizes=u1,16