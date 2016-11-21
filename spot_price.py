import boto3
from datetime import datetime

client = boto3.client('ec2')

def get_current_spot_price():
    response = client.describe_spot_price_history(
        #StartTime=datetime(2016, 11, 20, 22, 36, 8, 762629),
        #EndTime=datetime(2016, 11, 20, 22, 36, 8, 762629),
        StartTime = datetime.now(),
        EndTime = datetime.now(),
        InstanceTypes = ['m3.medium',],
        ProductDescriptions = ['Linux/UNIX (Amazon VPC)',],
        AvailabilityZone = 'us-west-2a')
    return response


print get_current_spot_price()
