import configparser
import boto3
import time
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

def create_cluster(emr):
    """
    Create EMR CLuster
    :param emr: emr boto resource
    :return:
    """
    root_bucket = config.get('S3_CONF', 'ROOT_BUCKET')
    input_data = config.get('S3_CONF', 'INPUT_DATA')
    step_app_name =  config.get('CLUSTER_CONF', 'STEP_APP_NAME')

    args = [
        'spark-submit',
        '/home/hadoop/pyspark_code/etl.py',
        '--root_bucket', 's3a://' + root_bucket,
        '--input_data', input_data,
        '--step_app_name', step_app_name,
    ]

    cluster_name = config.get('CLUSTER_CONF', 'CLUSTER_NAME')
    bootstrap_file = config.get('CLUSTER_CONF', 'BOOTSTRAP_FILE')
    logging_bucket = config.get('CLUSTER_CONF', 'LOGGING_BUCKET')
    jobflow_role = config.get('CLUSTER_CONF', 'JOBFLOWROLE')
    service_role = config.get('CLUSTER_CONF', 'SERVICEROLE')
    ec2subnet = config.get('CLUSTER_CONF', 'EC2SUBNET')

    JOB_FLOW_OVERRIDES = {
       "Name": cluster_name,
       "LogUri": logging_bucket,
       "ReleaseLabel": "emr-5.30.1",
        # We want our EMR cluster to have HDFS and Spark
        "Applications": [
           {"Name": "Hadoop"},
           {"Name": "Spark"}
        ],
        "Configurations": [
            {
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "true"
                }
            }
        ],
        "Instances": {
            "Ec2SubnetId": ec2subnet,
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "SPOT",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core - 2",
                    "Market": "SPOT", # Spot instances are a "use as available" instances
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 2,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": False, # this terminates the cluster after all steps are completed
            "TerminationProtected": False, # this lets us programmatically terminate the cluster
        },
        "VisibleToAllUsers": True,
        "JobFlowRole": jobflow_role,
        "ServiceRole": service_role,
        "BootstrapActions": [
          {
             "Name": "copy scripts",
             "ScriptBootstrapAction": {
                "Path": bootstrap_file
             }
          }
       ]
    }


    response_run_job_flow = emr.run_job_flow(
       Name = JOB_FLOW_OVERRIDES['Name'],
       LogUri = JOB_FLOW_OVERRIDES['LogUri'],
       ReleaseLabel = JOB_FLOW_OVERRIDES['ReleaseLabel'],
       Instances = JOB_FLOW_OVERRIDES['Instances'],
       Applications = JOB_FLOW_OVERRIDES['Applications'],
       Configurations = JOB_FLOW_OVERRIDES['Configurations'],
       VisibleToAllUsers = JOB_FLOW_OVERRIDES['VisibleToAllUsers'],
       JobFlowRole = JOB_FLOW_OVERRIDES['JobFlowRole'],
       ServiceRole = JOB_FLOW_OVERRIDES['ServiceRole'],
       BootstrapActions = JOB_FLOW_OVERRIDES['BootstrapActions'],
       Steps=[{
                'Name': step_app_name,
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': args
                }
            }
        ]
    )

    if response_run_job_flow['ResponseMetadata']['HTTPStatusCode'] != 200:
       raise Exception(f'Bad Response! Response Code:{response_run_job_flow["ResponseMetadata"]["HTTPStatusCode"]}')

    job_flow_id = response_run_job_flow['JobFlowId']
    return job_flow_id

def get_cluster_status(emr, job_flow_id):
    """
    Check Redshift cluster status
    :param redshift: a redshift client instance
    :param cluster_identifier: Cluster unique identifier
    :return: Cluster status
    """
    response = emr.describe_cluster(ClusterId=job_flow_id)
    cluster_status = response['Cluster']['Status']['State']
    print(f'Cluster status : {cluster_status.upper()}')
    return cluster_status.upper()


def state(emr, job_flow_id):
    response_describe_cluster = emr.describe_cluster(
        ClusterId=job_flow_id
    )
    start_tz = response_describe_cluster['Cluster']['Status']['Timeline']['CreationDateTime']
    start = start_tz.replace(tzinfo=None)
    while get_cluster_status(emr, job_flow_id) not in ['TERMINATED', 'TERMINATING', 'TERMINATED_WITH_ERRORS', 'WAITING']:
        now = datetime.today()
        elapsedTime = now - start
        print(f'Cluster running time: {elapsedTime}')
        time.sleep(30)

    if get_cluster_status(emr, job_flow_id) == 'WAITING':
        raise Exception(' Manually terminate cluster!')

    get_cluster_status(emr, job_flow_id)

def main_emr_createsubmit():
    emr = boto3.client(
       'emr',
       region_name = 'eu-west-1',
       aws_access_key_id = config.get('AWS', 'AWS_ACCESS_KEY_ID'),
       aws_secret_access_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    )
    job_flow_id = create_cluster(emr)
    state(emr, job_flow_id)