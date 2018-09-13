import pandas as pd
import logging
import yaml
import boto3
import json


class Solution:
    logger = None
    config = None
    response = None

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.response = {'error': True, 'response': ''}

    def excelToJson(self, filename, sheet_name):
        self.logger.info(f'reading excel file | {filename}')
        df = pd.read_excel(filename, sheet_name=sheet_name)
        data = df.to_json(orient='records')
        del df
        try:
            self.logger.info(f"writing json to file | {self.config['json_filename']}")
            with open(self.config['json_filename'], 'w+') as f:
                f.write(data)
        except Exception as e:
            msg = f"cannot write into {self.config['json_filename']} file | {e.message}"
            self.logger.error()
            self.response['response'] = msg

        # upload json file to s3 bucket
        return self.uploadJson('s.json')
        # self.response['response'] = s3_url
        # self.response['error'] = False

    def uploadJson(self, filepath):
        self.logger.debug('calling AWS S3 service')
        try:
            s3 = boto3.client('s3',
                              aws_access_key_id=self.config['aws_access_key'],
                              aws_secret_access_key=self.config['aws_secret_access_key'])
        except Exception as e:
            msg = f"not able to call AWS S3 service | {e.message}"
            self.logger.error(msg)
            self.response['response'] = msg
            return self.response

        self.logger.info(f"Uploading {filepath} object to S3")

        try:
            with open(filepath, 'rb') as f:
                s3.put_object(Bucket=self.config['bucket_name'],
                              Key=filepath,
                              Body=f.read(),
                              ACL='public-read'
                              )
            self.response[
                'response'] = f"https://s3-us-west-2.amazonaws.com/{self.config['bucket_name']}/{self.config['json_object_keyname']}"
            self.response['error'] = False
        except Exception as e:
            msg = f"not able to upload json file to {self.config['bucket_name']} bucket | {e.message}"
            self.logger.error(msg)
            self.response['response'] = msg

        return self.response


def loadConfig(filename, logger):
    try:
        with open(filename) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f'not able to load the config file | {e.message}')
        config = False
    return config


def loadLogger():
    logger = logging.getLogger('solution')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    return logger


def lambda_handler(event, context):
    logger = loadLogger()
    logger.info('logger loaded')
    config = loadConfig('config.yaml', logger)
    if not config:
        return {'response': 'not able to extract the credentials', 'error': True}

    logger.info('config loaded')
    sol = Solution(config, logger)
    return json.dumps(sol.excelToJson('ISO10383_MIC.xls', 'MICs List by CC'))


if __name__ == '__main__':
    print(lambda_handler(1, 2))
