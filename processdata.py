import datetime
import glob
import re

import pandas as pd


def mergefiles(path='D:\py-dev\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports', extension='csv'):
    # Remove any . used in extension
    extension = re.sub('[\.]', '', extension)
    # Create final file path to scan
    finalpath = path + '\\*.' + extension
    # Find the files which are present
    files = glob.glob(finalpath)
    # Create empty list to be used in for loop
    df = []
    # For loop to find the files and process them
    for f in files:
        temp = pd.read_csv(f)
        date_time_str = f.split('\\')[-1].split('.')[0].split('_')[0]
        date_time_obj = datetime.datetime.strptime(date_time_str, '%m-%d-%Y').date()
        temp['created date'] = date_time_obj
        df.append(temp)

    # Return the concatenated files
    return pd.concat(df, ignore_index=True)
