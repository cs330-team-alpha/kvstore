import boto3
from datetime import datetime

client = boto3.client('ec2')

# returns a list of (spot price, AZ) 
def get_current_spot_price():
    response = client.describe_spot_price_history(
        #StartTime=datetime(2016, 11, 20, 22, 36, 8, 762629),
        #EndTime=datetime(2016, 11, 20, 22, 36, 8, 762629),
        StartTime = datetime.now(),
        EndTime = datetime.now(),
        InstanceTypes = ['m3.medium',],
        ProductDescriptions = ['Linux/UNIX (Amazon VPC)',])
        #AvailabilityZone = 'us-west-2a')
    history = response['SpotPriceHistory']
    results = [ ] 
    for e in history:
        results.append((e['SpotPrice'], e['AvailabilityZone']))
    return results

# p = get_current_spot_price()
# print '[%s]' % ', '.join(['(%s, %s)' % (s,a) for (s,a) in p])
