import boto3
import csv
import time
import experimental_bids
from datetime import datetime


IMAGE_ID = 'ami-4d87fc5a'
KEY_NAME = 'cs330'
SECURITY_GROUP_ID = 'sg-0e5f5174'
EXPERIMENT_DURATION = 24 * 60 * 60
LOG_PREFIX = 'results/' + datetime.now().strftime('%Y%m%d_%H%M%S')


# List of bids - kust of dicts containing bid, ami, securitygroupid, type, zone
bids = [{'ami': 'ami-df3779bf',
         'price': '0.00625',
         'sgid': 'sg-dc2124b8',
         'type': 't1.micro',
         'zone': 'us-west-1b'},
        ]


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
    Returns a dictionary containg the state and status of a spot instance request id
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


def launch_spot_bids(ec2client, bids):
    """
    Launch a set of spot bids from a list of bids. returns a dictionary containing active spot 
    requests
    """
    active_spot_requests = {}
    for bid in bids:
        request_time = datetime.now()
        print "Launching Bid: ", str(bid), str(request_time)
        response = launch_spot_instance_request(ec2client, bid, debug=False)
        assert type(
            response) is str, ("Request Error!. Response dump:", str(response))
        active_spot_requests[response] = {
            'spot_request_id': response,
            'bid_price': bid['price'],
            'zone': bid['zone'],
            'type': bid['type'],
            'request_time': request_time
        }
        dump_dict_csv(active_spot_requests, LOG_PREFIX + '_requests.log')
    return active_spot_requests


def get_fulfilled_spot_instances(ec2client, active_spot_requests):
    '''
    Check for fulfillment of spot requests ina  loop
    Returns a dictionary of fulfilled and failed spot requests
    '''

    fulfilled_spot_requests = {}
    failed_spot_requests = {}

    while len(active_spot_requests) > 0:
        for spot_request_id in active_spot_requests.keys():
            print "Checking Request", spot_request_id
            response = spot_request_status(ec2client, spot_request_id)
            assert type(
                response) is dict, ("Spot Error!. Response dump:", str(response))
            state = response['state'].strip()
            # print "Current State: ", state
            if state == 'active':
                print "Request ", spot_request_id, " launched..."
                fulfilled_request = active_spot_requests.pop(spot_request_id)
                fulfilled_request['launch_time'] = datetime.now()
                fulfilled_spot_requests[spot_request_id] = fulfilled_request
                dump_dict_csv(fulfilled_spot_requests,
                              LOG_PREFIX + '_success.log')
            # Request Failed
            elif state != 'open' or response['status']['Code'] == 'price-too-low':
                print "Request", spot_request_id, "failed..."
                print "Response: ", response
                print "State:", state
                failed_request = active_spot_requests.pop(spot_request_id)
                failed_request['status_code'] = response['status']['Code']
                failed_request['message'] = response['status']['Message']
                failed_request['fail_time'] = datetime.now()
                failed_spot_requests[
                    spot_request_id] = failed_request
                dump_dict_csv(failed_spot_requests, LOG_PREFIX + '_failed.log')

        # Sleep for 1 second before making further updates
        time.sleep(1)

    return fulfilled_spot_requests, failed_spot_requests


def do_experiment():
    ''' 
    LAB 2 experiment, launches a set of bids described in bids
    and monitors their execution for termination
    '''

    # Filter by instances that are not named controller
    # filters = [{'Name': 'tag:Name', 'Values': ['!Controller']}]
    # ec2resource = boto3.resource('ec2')
    ec2client = boto3.client('ec2')

    killed_spot_instances = {}

    experiment_start = datetime.now()

    print "Experiment Started: ", experiment_start

    active_spot_requests = launch_spot_bids(ec2client, experimental_bids.bids)
    print "Waiting for requests to propogate"
    time.sleep(5)
    fulfilled_spot_requests, failed_spot_requests = get_fulfilled_spot_instances(
        ec2client, active_spot_requests)

    # instance.launch_time

    print "Fulfilled: ", fulfilled_spot_requests
    print "Failed: ", failed_spot_requests

    elapsed_time = datetime.now() - experiment_start
    last_printed = 0

    print "Monitoring Instances for Termination"
    while len(fulfilled_spot_requests) > 0 and \
            elapsed_time.total_seconds() < EXPERIMENT_DURATION:
        for spot_request_id in fulfilled_spot_requests.keys():
            response = spot_request_status(ec2client, spot_request_id)
            assert type(response) is dict, \
                ("Spot Error!. Response dump:", str(response))
            if response['state'] != 'active':
                print spot_request_id, "killed..."
                killed_instance = fulfilled_spot_requests.pop(spot_request_id)
                killed_instance['kill_time'] = datetime.now()
                killed_instance['status_code'] = response['status']['Code']
                killed_instance['message'] = response['status']['Message']
                killed_spot_instances[spot_request_id] = killed_instance
                dump_dict_csv(killed_spot_instances,
                              LOG_PREFIX + '_killed.log')
        time.sleep(5)
        elapsed_time = datetime.now() - experiment_start
        last_printed += 1
        if(last_printed > 100):
            print "Waiting, Hours left in experiment:" + str((EXPERIMENT_DURATION - elapsed_time.total_seconds()) / (60 * 60))
            last_printed = 0
    print "Experiment Finished..."

if __name__ == '__main__':
    do_experiment()
