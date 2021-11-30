from io import BytesIO
import boto3
from botocore.exceptions import ClientError
import logging
import awswrangler as wr

class S3Util():
    """
    Performs S3 commands
    """

    sts_client = boto3.client('sts')    

    def __init__(self, s3_role_arn = None, logger=None):
        """
        Arguments:
            s3_role_arn -- role to assume
            logger -- (optional)

        Returns:
            None

        """
        self.__logger = logging.getLogger(self.__class__.__name__) if logger is None else logger

        if s3_role_arn is not None and len(s3_role_arn)>0:
            self.assumed_role_object=self.sts_client.assume_role(
                RoleArn=s3_role_arn,
                RoleSessionName="AssumeRoleSession1"
            )
            credentials = self.assumed_role_object['Credentials']

            # Use the temporary credentials that AssumeRole returns to make a 
            # connection to Amazon S3  
            self.s3_resource = boto3.resource(
                's3',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        else:
            self.s3_resource = boto3.resource('s3')


    def check_file_exists(self, s3_bucket, s3_key):
        """
        Checks if file exists in S3
        """
        return bool(list(self.s3_resource.Bucket(s3_bucket).objects.filter(Prefix=s3_key)))


    def upload_file(self, s3_bucket, s3_key, file_path):
        """
        Saves dataframe to S3 in parquet format

        :param s3_bucket: bucket where data will be saved to
        :param s3_key: s3 key for the uploaded file
        :param data_bytes: file data
        """
        try:
            # Filename - File to upload
            # Bucket - Bucket to upload to (the top level directory under AWS S3)
            # Key - S3 object name (can contain subdirectories). If not specified then file_name is used
            self.s3_resource.meta.client.upload_file(Filename=file_path, Bucket=s3_bucket, Key=s3_key)
        except ClientError as ex:
            self.__logger.error(f'Exception while uploading file to S3: {ex}. S3 path: {s3_bucket/s3_key}')
            raise ex

    
    def read_csv(self, path_to_file):
        try:
            return wr.s3.read_csv(path_to_file, skip_blank_lines=True)
        except Exception as ex:
            self.__logger.error(f'Exception while reading csv from S3: {ex}. S3 path: {path_to_file}')
            raise ex

    def write_csv(self, df, path_to_file):
        try:
            wr.s3.to_csv(df=df, index=False, path=path_to_file)
        except Exception as ex:
            self.__logger.error(f'Exception while saving dataframe to S3: {ex}. S3 path: {path_to_file}')
            raise ex