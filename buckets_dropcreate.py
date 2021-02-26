import configparser
import boto3

config = configparser.ConfigParser()
config.read('dl.cfg')

def dropcreate_bucket(s3, root_bucket):
    bucket = s3.Bucket(root_bucket)
    # Delete bucket if exists
    if bucket in s3.buckets.all():
        print(f'Bucket exists: {root_bucket}')
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
        print(f'Bucket deleted: {root_bucket}')

    # Create the root_bucket
    try:
        s3.create_bucket(ACL='private', Bucket=root_bucket,
                         CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        print(f'Bucket created: {root_bucket}')
    except Exception as e:
        raise Exception(f'failed to create bucket: {root_bucket}. Exception:{e}')

    return bucket

def create_keys(tables, bucket):
    # Create folders for the tables
    for table in tables:
        try:
            bucket.put_object(Key='tables/'+table)
            print(f'Folder/Key created: {table}')
        except Exception as e:
            raise e


def upload_file(bucket):
    conf = {
        'emr_boostrap.sh': 'pyspark_code/bootstrap/emr_boostrap.sh',
        'etl.py': "pyspark_code/code/etl.py",
        'dl.cfg': "pyspark_code//dl.cfg"
    }
    for file, s3key in conf.items():
        bucket.upload_file(file, s3key)
        print(f'upload file ; {file}')
    print('All files are ready to be executed!')

def main_bucket_dropcreate():
    tables = ['songs/', 'artists/', 'users/', 'time/', 'songplays/']
    access_key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    secret_access_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    root_bucket = config.get('S3_CONF', 'ROOT_BUCKET')
    s3 = boto3.resource(
        's3',
        region_name='eu-west-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )
    bucket = dropcreate_bucket(s3, root_bucket)
    create_keys(tables, bucket)
    upload_file(bucket)