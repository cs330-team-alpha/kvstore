import boto3
import csv
import time
from datetime import datetime
import subprocess

# TODO: Cleanup

IMAGE_ID = 'ami-4d87fc5a'
KEY_NAME = 'cs330'
SECURITY_GROUP_ID = 'sg-0e5f5174'
EXPERIMENT_DURATION = 24 * 60 * 60
LOG_PREFIX = 'results/' + datetime.now().strftime('%Y%m%d_%H%M%S')

DEFAULT_MEMCACHED_PORT = 11211


### For Local Testing

LOCAL_PORTS = [
    12000,
    12001,
    12002,
    12003,
    12004,
    12005,
]


def launch_local_node(node_id):
    ''' Launch a local memcached node from a list of ports
        # /usr/bin/memcached -m 64 -p 11211 -u memcache -l 127.0.0.1
    '''
    port_string = str(LOCAL_PORTS[node_id])
    subprocess.Popen(["memcached", "-m 64", "-u memcache",
                      "-l 127.0.0.1", "-p ", port_string])
    return 'localhost', port_string


def launch_spot_instance_request(ec2client, launch_config, debug=False):
    """
    Given an ec2 client and launch_config, send a launch request to EC2
    Returns a string containing an spot-request ID on succcess,
    Returns a dictionary of EC2 response on failure.
    """
    response = ec2client.request_spot_instances(
        DryRun=debug,
        SpotPrice=launch_config['price'],
        LaunchSpecification={
            'ImageId': launch_config['ami'],
            'KeyName': KEY_NAME,
            'InstanceType': launch_config['type'],
            'Placement': {
                'AvailabilityZone': launch_config['zone'],
            },
            'SecurityGroupIds': [
                launch_config['sgid'],
            ]
        },
    )
    try:
        return response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
    except:
        return response


def spot_request_status(ec2client, spot_request_id):
    """
    Returns a dictionary containg the state and status of a spot
    instance request id
    Returns a dictionary containing the EC2 response on failure.
    """
    response = ec2client.describe_spot_instance_requests(
        SpotInstanceRequestIds=[spot_request_id])
    try:
        return {'state': response['SpotInstanceRequests'][0]['State'],
                'status': response['SpotInstanceRequests'][0]['Status']}
    except:
        return response


def get_spot_instance_id(ec2client, spot_request_id):
    """
    Get the actual instance ID of a fullfilled spot request
    """
    response = ec2client.describe_spot_instance_requests(
        SpotInstanceRequestIds=[spot_request_id])
    try:
        return response['SpotInstanceRequests'][0]['InstanceId']
    except:
        return response


def get_instance_by_id(ec2client, instance_id):
    """
    Get an EC2 instance object by ID
    """
    return [instance for instance
            in ec2client.instances.filter(InstanceIds=[instance_id])][0]


def dump_dict_csv(full_dict, csvfile):
    """
    Function to dump a dictionary to a CSV - Useful for collecting results.
    """
    with open(csvfile, 'wb') as f:
        for key, my_dict in full_dict.iteritems():
            w = csv.DictWriter(f, my_dict.keys())
            w.writeheader()
            w.writerow(my_dict)


def launch_spot_node(bid_price):
    bid = {'ami': 'ami-104d0507',
           'price': bid_price,
           'sgid': 'sg-dc2124b8',   # VERIFY SGID and that port 11211 is open
           'type': 't2.micro',
           'zone': 'us-east-1b'
           }

    ec2client = boto3.client('ec2')
    spot_request_id = launch_spot_instance_request(ec2client, bid)
    print "Waiting for requests to propogate"
    time.sleep(5)
    waiting = True

    while waiting:
        print "Checking Request", spot_request_id
        response = spot_request_status(ec2client, spot_request_id)
        assert type(
            response) is dict, ("Spot Error!. Response dump:", str(response))
        state = response['state'].strip()
        # print "Current State: ", state
        if state == 'active':
            print "Request ", spot_request_id, " launched..."
            launched_instance_id = get_spot_instance_id(spot_request_id)
            waiting = False

        # Request Failed
        elif state != 'open' or response['status']['Code'] == 'price-too-low':
            print "Request", spot_request_id, "failed..."
            print "Response: ", response
            print "State:", state
            return None
        time.sleep(1)

    instance = get_instance_by_id(launched_instance_id)

    instance.wait_until_running()

    # Reload the instance attributes
    instance.load()
    return instance.public_dns_name, DEFAULT_MEMCACHED_PORT
