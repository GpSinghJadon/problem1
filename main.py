import pandas as pd

def solution():
    xcel_filepath = 'ISO10383_MIC.xls'
    df = pd.read_excel(xcel_filepath, sheet_name='MICs List by CC')
    return df.to_json(orient='records')


if __name__ == '__main__':
    print(solution())