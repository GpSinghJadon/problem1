import pandas as pd
import logging
import yaml
import boto3
import io


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
        data = pd.read_excel(filename, sheet_name=sheet_name).to_json(orient='records')
        return self.uploadJson(data)

    def uploadJson(self, data):
        self.logger.debug('calling AWS S3 service')
        try:
            s3 = boto3.client('s3',
                              aws_access_key_id=self.config['aws_access_key'],
                              aws_secret_access_key=self.config['aws_secret_access_key'])
            data = io.BytesIO(bytearray(data, encoding='utf-8'))
            s3.put_object(Bucket=self.config['bucket_name'], Key=self.config['json_object_keyname'], Body=data,
                          ACL='public-read')
            self.response[
                'response'] = f"https://s3-us-west-2.amazonaws.com/{self.config['bucket_name']}/{self.config['json_object_keyname']}"
            self.response['error'] = False
        except Exception as e:
            msg = f"write json object in S3 | {e}"
            self.logger.error(msg)
            self.response['response'] = msg
        return self.response


def loadConfig(filename, logger):
    try:
        with open(filename) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f'not able to load the config file | {e}')
        config = False
    return config


def loadLogger():
    logger = logging.getLogger('solution')
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def lambda_handler(event, context):
    logger = loadLogger()
    logger.info('logger loaded')
    config = loadConfig('config.yaml', logger)
    if not config:
        return {'response': 'not able to extract the credentials', 'error': True}

    logger.info('config loaded')
    sol = Solution(config, logger)
    return sol.excelToJson('ISO10383_MIC.xls', 'MICs List by CC')


if __name__ == '__main__':
    print(lambda_handler(1, 2))
