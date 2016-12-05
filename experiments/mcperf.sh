#/bin/bash

mcperf --linger=0 --timeout=300 --conn-rate=100 --call-rate=1000 --num-calls=100000000 --num-conns=100 --sizes=u1,16 -s ec2-52-207-231-196.compute-1.amazonaws.com -p 11211 --method='get'
