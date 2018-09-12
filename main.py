import pandas as pd
import logging
import yaml
import boto3


class Solution:
    logger = None
    config = None

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config

    def excelToJson(self, filename, sheet_name):
        df = pd.read_excel(filename, sheet_name=sheet_name)
        data = df.to_json(orient='records')
        print(self.config)
        try:
            with open(self.config['json_filename'], 'w+') as f:
                f.write(data)
        except Exception as e:
            self.logger.error(f"cannot write into {self.config['json_filename']} file | {e.message}")

        # upload s.json file to s3 bucket
        self.uploadJson('s.json')

    def uploadJson(self, filepath):
        self.logger.debug('uploading json file to s3')
        try:
            s3 = boto3.client('s3',
                              aws_access_key_id=self.config['aws_access_key'],
                              aws_secret_access_key=self.config['aws_secret_access_key'])
        except Exception as e:
            self.logger.error(f'not able to call AWS S3 service | {e.message}')

        self.logger.info(f"Uploading {filepath} object to S3")

        try:
            with open(filepath, 'r') as f:
                s3.put_object(Bucket=self.config['bucket_name'],
                              Key=filepath,
                              Body=f.read()
                              )
        except Exception as e:
            self.logger.error(f"not able to upload json file to {self.config['bucket_name']} bucket | {e.message}")


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
    return logger


if __name__ == '__main__':
    logger = loadLogger()
    config = loadConfig('config.yaml', logger)
    sol = Solution(config, logger)
    sol.excelToJson('ISO10383_MIC.xls', 'MICs List by CC')
