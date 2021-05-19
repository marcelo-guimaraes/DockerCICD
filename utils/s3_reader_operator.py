import boto3
import s3fs
import pandas as pd

"""
    Class S3ReaderOperator: reads extracted(s) file(s) on a S3 bucket.

    -> PARAMS:
        - extraction_path: path of the file that will be read
        - input_format: format of the file that will be read
            -> Default: .csv
        - bucket_name: name of the bucket to read on.
            -> Default: "s3://health-lake-input"

    -> METHODS:
        - _handle_path: replace bucket name repetitions from path
        - _check_format: filters so that only the files will be read on the dataframe
        - read_from_bucket: iterates over the files from bucket and returns a merged dataframe
"""

class S3ReaderOperator:
    def __init__(self, extraction_path, input_format, bucket_name):
        self.role = "arn:aws:iam::429201177306:role/S3Reading_Role"
        self.bucket_name = bucket_name
        self.extraction_path = self._handle_path(extraction_path)
        self.input_format = input_format

        self.sts_connection = boto3.client('sts')   
        self.account_infos = self.sts_connection.assume_role(
            RoleArn=self.role,
            RoleSessionName="read_object_from_s3"
        )
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=self.account_infos['Credentials']['AccessKeyId'],
            aws_secret_access_key=self.account_infos['Credentials']['SecretAccessKey'],
            aws_session_token=self.account_infos['Credentials']['SessionToken']
        )

    def _handle_path(self, extraction_path):
        return extraction_path.replace(f"{self.bucket_name}/", '').replace('s3://', '') \
            if '/' not in extraction_path[0] else extraction_path[1:]

    def _check_format(self, key):
        return key if self.input_format in key else None

    def read_from_bucket(self):
        bucket = self.s3.Bucket(self.bucket_name)
        prefix_objs = bucket.objects.filter(Prefix=self.extraction_path)
        files = [obj.key for obj in prefix_objs]
        frames = []

        for key_file in list(filter(self._check_format, files)):
            file_path = f"s3://{self.bucket_name}/{key_file}"
            print(f"File path: {file_path}")
            csv_file = pd.read_csv(file_path)
            frames.append(csv_file)
        
        data = pd.concat(frames)
        return data

def HandlerS3Reader(extraction_path, input_format='csv', bucket='health-lake-input'):
    print("============================ STARTING READING PROCCESS ============================")
    _bucket_name = bucket or 'health-lake-input'
    _input_format = input_format or 'csv'
    S3ReaderOperator(extraction_path, _input_format, _bucket_name).read_from_bucket()
