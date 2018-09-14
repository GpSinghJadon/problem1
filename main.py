import pandas as pd
import logging
import yaml
import boto3
import io
import requests


class Solution:
    logger = None
    config = None
    response = None

    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.response = {'error': True, 'response': ''}

    # main function which will handle all the processing
    def excelToJson(self, sheet_name):
        # download the excel to the tile path specified in config file
        if self.downloadExcel():
            self.logger.info(f"reading excel file | {self.config['excel_filepath']}")
            # read the excel sheet and convert the sheet to json
            data = pd.read_excel(self.config['excel_filepath'], sheet_name=sheet_name).to_json(orient='records')
            # upload the json data to S3 object
            return self.uploadJson(data)
        else:
            self.response['response'] = f"not able to write excel file from the URL"
            return self.response

    def uploadJson(self, data):
        self.logger.debug('calling AWS S3 service')
        # make client of aws S3
        try:
            s3 = boto3.client('s3',
                              aws_access_key_id=self.config['aws_access_key'],
                              aws_secret_access_key=self.config['aws_secret_access_key'])
            # convert the string utf-8 to bytes for uploading to S3 bucket
            data = io.BytesIO(bytearray(data, encoding='utf-8'))

            # upload the data to s3 bucket
            s3.put_object(Bucket=self.config['bucket_name'], Key=self.config['json_object_keyname'], Body=data,
                          ACL='public-read')
            self.response[
                'response'] = f"https://s3-{self.config['aws_region']}.amazonaws.com/{self.config['bucket_name']}/{self.config['json_object_keyname']}"
            self.response['error'] = False
        except Exception as e:
            msg = f"write json object in S3 | {e}"
            self.logger.error(msg)
            self.response['response'] = msg
        return self.response

    def downloadExcel(self):
        try:
            # download the excel file from given URL
            r = requests.get(self.config['excel_url'], allow_redirects=True)

            # write the file in the temp directory
            open(self.config['excel_filepath'], 'wb').write(r.content)
            return True
        except Exception as e:
            self.logger.info(f"not able to download excel data to xls file | {e}")
            return False

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
    # load config data from the config.yaml file
    config = loadConfig('config.yaml', logger)
    if not config:
        return {'response': 'not able to extract the credentials', 'error': True}

    logger.info('config loaded')
    sol = Solution(config, logger)
    return sol.excelToJson('MICs List by CC')


if __name__ == '__main__':
    # for debugging purpose
    print(lambda_handler(1, 2))
