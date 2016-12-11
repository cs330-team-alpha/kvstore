import boto3
import csv
import time
from datetime import datetime, timedelta
from operator import itemgetter
import subprocess
import psutil

PROCNAME = "python.exe"

# TODO: Cleanup

IMAGE_ID = 'ami-34ede923'
KEY_NAME = 'cs330'
SECURITY_GROUP_ID = 'sg-d791d2aa'
EXPERIMENT_DURATION = 24 * 60 * 60
LOG_PREFIX = 'results/' + datetime.now().strftime('%Y%m%d_%H%M%S')

DEFAULT_MEMCACHED_PORT = 11211

TIMING_GRANULARITY = 5  # Minutes

# For Local Testing
LOCAL_PORTS = 12000
LOCAL_PIDS = {}


def launch_local_node(node_id):
    ''' Launch a local memcached node from a list of ports
        # /usr/bin/memcached -m 64 -p 11211 -u memcache -l 127.0.0.1
    '''

    memcached = "memcached -m 64 -u memcache -l %s -p %d"
    port = LOCAL_PORTS + node_id
    process = subprocess.Popen(memcached % ('localhost', port), shell=True)
    LOCAL_PIDS[node_id] = process.pid  # Store PID for killing in future
    print "Launched local node at port " + str(port) + " PID: " + str(process.pid)
    return 'localhost', str(port)


def kill_local_node(node_id):
    ''' Kill local memcached process by PID'''
    p = psutil.Process(LOCAL_PIDS[node_id])
    p.terminate()
    del LOCAL_PIDS[node_id]


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
    bid = {'ami': IMAGE_ID,
           'price': str(bid_price),
           'sgid': SECURITY_GROUP_ID,
           'type': 'm3.medium',
           'zone': 'us-east-1b'
           }
    ec2resource = boto3.resource('ec2')
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
            launched_instance_id = get_spot_instance_id(ec2client, spot_request_id)
            waiting = False

        # Request Failed
        elif state != 'open' or response['status']['Code'] == 'price-too-low':
            print "Request", spot_request_id, "failed..."
            print "Response: ", response
            print "State:", state
            return None
        time.sleep(1)

    instance = get_instance_by_id(ec2resource, launched_instance_id)

    instance.wait_until_running()

    # Reload the instance attributes
    instance.load()

    # TODO: Ping and verify running memcached instance
    time.sleep(30)

    return instance.public_dns_name, DEFAULT_MEMCACHED_PORT


def kill_spot_node(addr):
    # TODO Implement  function by killing instance
    # and spot request through nodeid
    pass


def get_node_load(cwclient, instance_id):
    # Eg: cwclient = boto3.client('cloudwatch')
    now = datetime.utcnow()
    past = now - timedelta(minutes=TIMING_GRANULARITY)
    future = now + timedelta(minutes=TIMING_GRANULARITY)

    results = cwclient.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName='CPUUtilization',
        Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
        StartTime=past,
        EndTime=future,
        Period=300,  # TODO: Verify timing period
        Statistics=['Average'])

    datapoints = results['Datapoints']
    last_datapoint = sorted(datapoints, key=itemgetter('Timestamp'))[-1]
    utilization = last_datapoint['Average']
    load = round((utilization / 100.0), 2)

    return load


def probe_cluster_load(cwclient, instance_ids, upper_threshold):
    hot_nodes = []
    for instance in instance_ids:
        node_load = get_node_load(cwclient, instance_ids)
        if node_load > upper_threshold:
            hot_nodes.append(instance)
    return hot_nodes
