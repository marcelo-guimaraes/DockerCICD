import boto3
import sys
from datetime import datetime

"""
    Class S3WriterOperator: writes an extracted file on a S3 bucket.

    -> PARAMS:
        - extraction_file: path of the file extracted on your script
        - extraction_name: name of the file extracted who will be written
        - extraction_source: name of the source related to your extraction
        - bucket_name: name of the bucket to write on.
            -> Default: "s3://health-lake-input"

    -> METHODS:
        - write_on_bucket: puts the extracted file inside the bucket
"""

class S3WriterOperator:
    def __init__(self, extraction_file, extraction_name, extraction_source, bucket_name):
        self.role = "arn:aws:iam::429201177306:role/S3Extraction_Role"
        self.extraction_file = extraction_file
        self.extraction_name = extraction_name
        self.extraction_source = extraction_source
        self.bucket_name = bucket_name
        self.sts_connection = boto3.client('sts')   
        self.account_infos = self.sts_connection.assume_role(
            RoleArn=self.role,
            RoleSessionName="put_object_on_s3"
        )
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.account_infos['Credentials']['AccessKeyId'],
            aws_secret_access_key=self.account_infos['Credentials']['SecretAccessKey'],
            aws_session_token=self.account_infos['Credentials']['SessionToken']
        )

    def write_on_bucket(self):
        _year = datetime.today().year
        _month = datetime.today().month
        _day = datetime.today().day
        _extraction_path = f"raw/{self.extraction_source}/{_year}/{_month}/{_day}/{self.extraction_name}"
        print(f"YOUR EXTRACTION PATH IS: s3://{self.bucket_name}/{_extraction_path}")
        try:
            response = self.s3.put_object(
                Body=self.extraction_file,
                Bucket=self.bucket_name,
                Key=_extraction_path,
            )
        except:
            print(f"ERROR: {sys.exc_info()}")

def HandlerS3Writer(extracted_file, extraction_name, extraction_source, bucket='health-lake-input'):
    print("============================ STARTING WRITING PROCCESS ============================")
    _bucket_name = bucket or 'health-lake-input'
    print(f"BUCKET NAME: {_bucket_name}")
    print(f"EXTRACTION SOURCE: {extraction_source}")
    print(f"EXTRACTION FILE: {extraction_name}")
    
    S3WriterOperator(extracted_file, extraction_name, extraction_source, _bucket_name).write_on_bucket()
