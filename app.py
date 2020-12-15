import json
import boto3
import os

geo = os.environ['geo']
action = os.environ['action']


def lambda_handler(event, context):
    regionlist = get_regions(geo)
    tag_untag_resources(regionlist, action)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def get_regions(geo):
    americas = ['us-east-1', 'us-east-2', 'us-west-1',
                'us-west-2', 'ca-central-1', 'sa-east-1']
    europe = ['eu-central-1', 'eu-west-1', 'me-south-1',
              'eu-west-2', 'eu-west-3', 'eu-north-1']
    asiapac = ['ap-east-1', 'ap-south-1',
               'ap-northeast-3', 'ap-northeast-2', 'ap-southeast-1',
               'ap-southeast-2', 'ap-northeast-1']
    if geo == 'americas':
        return americas
    elif geo == 'europe':
        return europe
    elif geo == 'asiapac':
        return asiapac


def connect_service(region, service):
    '''connect client'''
    try:
        client = boto3.client(service, region_name=region)
        return client
    except Exception:
        pass


def tag_ebs_snaps(ec2, action):
    snapshots = {}
    for response in ec2.get_paginator('describe_snapshots').paginate(OwnerIds=['self']):
        snapshots.update([(snapshot['SnapshotId'], snapshot)
                          for snapshot in response['Snapshots']])
    if action == 'tag':
        for snapshot in snapshots.values():
            ec2.create_tags(Resources=[snapshot['SnapshotId']], Tags=[
                {
                    'Key': 'snap_usage',
                    'Value': 'ebs'
                },
            ]
            )
    elif action == 'untag':
        for snapshot in snapshots.values():
            ec2.delete_tags(Resources=[snapshot['SnapshotId']], Tags=[
                {
                    'Key': 'snap_usage',
                    'Value': 'ebs'
                },
            ]
            )


def tag_rds_snaps(rds, action):
    snapshots = {}
    for response in rds.get_paginator('describe_db_snapshots').paginate():
        snapshots.update([(snapshot['DBSnapshotArn'], snapshot)
                          for snapshot in response['DBSnapshots']])

    if action == 'tag':
        for snapshot in snapshots.values():
            arn = snapshot['DBSnapshotArn']
            rds.add_tags_to_resource(ResourceName=arn, Tags=[
                {
                    'Key': 'snap_usage',
                    'Value': 'rds'
                },
            ]
            )
    if action == 'untag':
        for snapshot in snapshots.values():
            arn = snapshot['DBSnapshotArn']
            rds.remove_tags_from_resource(
                ResourceName=arn, TagKeys=['snap_usage', ])


def tag_db_cluster_snaps(rds, action):
    snapshots = {}
    for response in rds.get_paginator('describe_db_cluster_snapshots').paginate():
        snapshots.update([(snapshot['DBClusterSnapshotArn'], snapshot)
                          for snapshot in response['DBClusterSnapshots']])
    if action == 'tag':
        for snapshot in snapshots.values():
            arn = snapshot['DBClusterSnapshotArn']
            rds.add_tags_to_resource(ResourceName=arn, Tags=[
                {
                    'Key': 'snap_usage',
                    'Value': 'rds'
                },
            ]
            )
    elif action == 'untag':
        for snapshot in snapshots.values():
            arn = snapshot['DBClusterSnapshotArn']
            rds.remove_tags_from_resource(
                ResourceName=arn, TagKeys=['snap_usage', ])


def tag_untag_resources(regionlist, action):
    for region in regionlist:
        ec2 = connect_service(region, 'ec2')
        tag_ebs_snaps(ec2, action)
        rds = connect_service(region, 'rds')
        tag_rds_snaps(rds, action)
        tag_db_cluster_snaps(rds, action)
