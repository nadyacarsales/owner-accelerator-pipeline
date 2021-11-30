import boto3
from botocore.exceptions import ClientError
import logging
import awswrangler as wr

from pandas.core.frame import DataFrame

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


    def __get_bucket_and_key_from_s3_path(self, path:str):
        prefix ='s3://'
        if not path:
            return '', ''
        path_no_prefix = path[len(prefix):] if path.startswith(prefix) else path
        bucket = path_no_prefix.split('/')[0]
        key = path_no_prefix[len(bucket)+1:]
        return bucket, key


    def read_text(self, path_to_file:str):
        try:
            bucket, key = self.__get_bucket_and_key_from_s3_path(path_to_file)
            return self.s3_resource.Object(bucket, key).get()['Body'].read().decode('utf-8') 
        except Exception as ex:
            self.__logger.error(f'Exception while reading csv from S3: {ex}. S3 path: {path_to_file}')
            raise ex


    def write_csv(self, df:DataFrame, path_to_file):
        try:
            wr.s3.to_csv(df=df, index=False, path=path_to_file)
        except Exception as ex:
            self.__logger.error(f'Exception while saving dataframe to S3: {ex}. S3 path: {path_to_file}')
            raise ex