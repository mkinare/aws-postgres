import sys

import psycopg2

import keyoperations


class DBOperations:
    def connect2db(self):
        """ Connect to the PostgreSQL database server """

        # Reading the key
        key = keyoperations.readkey()
        connection = None
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        try:
            # Define connection
            connection = psycopg2.connect(user=key['user'],
                                          password=key['password'],
                                          host=key['host'],
                                          port=key['port'],
                                          database=key['database'])

        except (Exception, psycopg2.Error) as error:
            print('Error while connecting to PostgreSQL...', error)
            sys.exit(1)

        return connection

    def populatedatabase(self, connection):
        """ Populate tables in the PostgreSQL database

        Current data table data types in pandas,

            Province_State            object
            Country_Region            object
            Last_Update       datetime64[ns]
            Confirmed                float64
            Deaths                   float64
            Recovered                float64
            Created_Date      datetime64[ns]
            Latitude                 float64
            Longitude                float64
        """
        commands = (
            """
            CREATE SCHEMA covid19data
            """,
            """
            CREATE TABLE covid19data.jobs (
                Insert_Date TIMESTAMP,
                Job_id FLOAT8,
                Last_File_Date TIMESTAMP
                ) PARTITION BY RANGE (Insert_Date)
            """,
            """ CREATE TABLE covid19data.daily_reports (
                Job_id FLOAT8,
                Province_State VARCHAR(255),
                Country_Region VARCHAR(255),
                Last_Update TIMESTAMP,
                Confirmed INT8,
                Deaths INT8,
                Recovered INT8,
                Created_Date TIMESTAMP,
                Latitude FLOAT8,
                Longitude FLOAT8,
                Inserted_Date TIMESTAMP
            ) PARTITION BY RANGE (Created_Date)
            """)

        cursor = connection.cursor()
        try:
            for command in commands:
                cursor.execute(command)
                # close communication with the PostgreSQL database server
            cursor.close()
            # commit the changes
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                connection.close()
