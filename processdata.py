import datetime
import glob
import re
import time

import pandas as pd


def mergefiles(path='D:\py-dev\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports', extension='csv'):
    # Remove any . used in extension
    extension = re.sub('[.]', '', extension)
    # Create final file path to scan
    finalpath = path + '\\*.' + extension
    # Find the files which are present
    files = glob.glob(finalpath)
    # Create empty list to be used in for loop
    df = []
    # For loop to find the files and process them
    for f in files:
        temp = pd.read_csv(f)
        # Get current time to create job id
        current_time = int(time.time())
        temp['Job_id'] = current_time
        # Rename some columns to remove special characters in column names
        temp.rename(columns={'Province/State': 'Province_State',
                             'Country/Region': 'Country_Region',
                             'Last Update': 'Last_Update'}, inplace=True)
        # Fill 0 instead of NaN or inf or NA in integer columns
        temp[['Confirmed', 'Deaths', 'Recovered']] = temp[['Confirmed', 'Deaths', 'Recovered']].fillna(value=0)
        # Create a column based on the file name
        date_time_str = f.split('\\')[-1].split('.')[0].split('_')[0]
        date_time_obj = datetime.datetime.strptime(date_time_str, '%m-%d-%Y').date()
        temp['Created_Date'] = date_time_obj
        # Converting columns to correct data types
        # Convert to string
        temp['Province_State'] = temp['Province_State'].astype(str)
        temp['Country_Region'] = temp['Country_Region'].astype(str)
        # Convert to date time
        try:
            temp['Last_Update'] = pd.to_datetime(temp['Last_Update'], format='%m/%d/%y %H:%M')
        except ValueError:
            try:
                temp['Last_Update'] = pd.to_datetime(temp['Last_Update'], format='%Y-%m-%dT%H:%M:%S')
            except ValueError:
                try:
                    temp['Last_Update'] = pd.to_datetime(temp['Last_Update'], format='%m/%d/%Y %H:%M')
                except ValueError as error:
                    print(error)
        # Convert to date
        temp['Created_Date'] = pd.to_datetime(temp['Created_Date'], format='%Y-%m-%d')
        temp['Created_Date'] = temp['Created_Date'].dt.normalize()
        # Convert to int32
        temp['Confirmed'] = temp['Confirmed'].astype('int32')
        temp['Deaths'] = temp['Deaths'].astype('int32')
        temp['Recovered'] = temp['Recovered'].astype('int32')

        df.append(temp)

    # Return the concatenated files
    return pd.concat(df, ignore_index=True)
