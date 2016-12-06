#/bin/bash

#mcperf --linger=0 --timeout=300 --conn-rate=100 --call-rate=100 --num-calls=10000 --num-conns=10 --sizes=u1,16 -s ec2-52-207-231-196.compute-1.amazonaws.com -p 11211 --method='get'
mcperf --linger=0 --timeout=300 --conn-rate=100 --call-rate=100 --num-calls=10000 --num-conns=10 --sizes=u1,16 -s localhost -p 8000 --method='get'
