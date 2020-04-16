import datetime
import glob
import re
import time
import pickle
import pandas as pd
import numpy as np
from datetime import datetime


def mergefiles(path='E:\py-dev\COVID-19\csse_covid_19_data\csse_covid_19_daily_reports', extension='csv'):
    # Reading the column names to run correct code
    pickle_in = open("columns.pickle", "rb")
    colnames = pickle.load(pickle_in)
    print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), 'Read column names dictionary')
    # Remove any . used in extension
    extension = re.sub('[.]', '', extension)
    # Create final file path to scan
    finalpath = path + '\\*.' + extension
    # Find the files which are present
    files = glob.glob(finalpath)
    # Create empty list to be used in for loop
    df = []
    # Get current time to create job id
    current_time = int(time.time())
    # For loop to find the files and process them
    for f in files:
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[START] Reading', f)
        temp = pd.read_csv(f)
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[FINISHED] Read', f)
        # Classify and process based on which columns were detected
        if temp.columns.to_list() == colnames[0]:
            # Print logs
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[INFO] Case 0 identified for', f)
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[START] Processing ', f)
            # Create missing columns
            temp['Latitude'] = np.nan
            temp['Longitude'] = np.nan
            temp['County'] = np.nan
            temp['Active'] = np.nan

            # Rename some columns to remove special characters in column names
            temp.rename(columns={'Province/State': 'Province_State',
                                 'Country/Region': 'Country_Region',
                                 'Last Update': 'Last_Update'}, inplace=True)
            # Fill 0 instead of NaN or inf or NA in integer columns
            temp[['Confirmed', 'Deaths', 'Recovered']] = temp[['Confirmed', 'Deaths', 'Recovered']].fillna(value=0)
            # Create a column based on the file name
            date_time_str = f.split('\\')[-1].split('.')[0].split('_')[0]
            date_time_obj = datetime.strptime(date_time_str, '%m-%d-%Y').date()
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
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[FINISHED] Processing ', f)
        elif temp.columns.to_list() == colnames[1]:
            # Print logs
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[INFO] Case 1 identified for', f)
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[START] Processing ', f)
            # Create missing columns
            temp['County'] = np.nan
            temp['Active'] = np.nan

            # Rename some columns to remove special characters in column names
            temp.rename(columns={'Province/State': 'Province_State',
                                 'Country/Region': 'Country_Region',
                                 'Last Update': 'Last_Update'}, inplace=True)
            # Fill 0 instead of NaN or inf or NA in integer columns
            temp[['Confirmed', 'Deaths', 'Recovered']] = temp[['Confirmed', 'Deaths', 'Recovered']].fillna(value=0)
            # Create a column based on the file name
            date_time_str = f.split('\\')[-1].split('.')[0].split('_')[0]
            date_time_obj = datetime.strptime(date_time_str, '%m-%d-%Y').date()
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
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[FINISHED] Processing ', f)
        elif temp.columns.to_list() == colnames[2]:
            # Print logs
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[INFO] Case 2 identified for', f)
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[START] Processing ', f)
            # Remove columns which we don't need
            temp.drop(columns=['FIPS', 'Combined_Key'], inplace=True)
            # Rename some columns to remove special characters in column names
            temp.rename(columns={'Admin2': 'County',
                                 'Lat': 'Latitude',
                                 'Long_': 'Longitude'}, inplace=True)
            # Fill 0 instead of NaN or inf or NA in integer columns
            temp[['Confirmed', 'Deaths', 'Recovered']] = temp[['Confirmed', 'Deaths', 'Recovered']].fillna(value=0)
            # Create a column based on the file name
            date_time_str = f.split('\\')[-1].split('.')[0].split('_')[0]
            date_time_obj = datetime.strptime(date_time_str, '%m-%d-%Y').date()
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
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[FINISHED] Processing ', f)
        else:
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[CRITICAL] No case for columns identified for', f)
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[ERROR] No Processing ', f)

    # Return the concatenated files
    if len(df) == 0:
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[ERROR] Final merge object is None')
    else:
        op = pd.concat(df, ignore_index=True)
        op['Inserted_Date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        op['Inserted_Date'] = pd.to_datetime(op['Inserted_Date'], format='%Y-%m-%d %H:%M:%S.%f')
        op['Active'].fillna(0, inplace=True)
        op['Active'] = op['Active'].astype('int32')

        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[SUCCESS] Final merge object returned')
        return op
