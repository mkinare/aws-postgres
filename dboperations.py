import sys
from datetime import datetime
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

    def executecommands(self, connection, commands):
        """ Execute the sql commands in the PostgreSQL database
        """
        cursor = connection.cursor()
        print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), 'Commands')
        print(*commands, sep="\n")
        print('Object type of commands object', type(commands))

        try:
            for command in commands:
                print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), 'SQL::')
                print(command)
                cursor.execute(command)
                print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[SUCCESS] SQL was executed')

                # close communication with the PostgreSQL database server
            cursor.close()
            # commit the changes
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[ERROR] ', error)
        # finally:
        #    if connection is not None:
        #        connection.close()

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
            CREATE SCHEMA covid19data;
            """,
            """
            CREATE TABLE covid19data.jobs (
                Insert_Date TIMESTAMP,
                Job_id FLOAT8,
                Last_File_Date TIMESTAMP
                ) PARTITION BY RANGE (Insert_Date);
            """,
            """ 
            CREATE TABLE covid19data.daily_reports (
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
            ) PARTITION BY RANGE (Created_Date);
            """)
        self.executecommands(connection, commands)
        return 'Database was populated'

    def dropschema(self, connection):
        """ Drop the schema if already exists PostgreSQL database
        """
        commands = (
            """
            DROP SCHEMA IF EXISTS covid19data CASCADE
            """,)
        self.executecommands(connection, commands)
        return 'Schema was dropped'

