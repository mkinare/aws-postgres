import sys
from datetime import datetime
import psycopg2
import keyoperations


class DBOperations:
    def connect2db(self):
        """
        Connect to the PostgreSQL database server
        """

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
        """
        Execute the sql commands in the PostgreSQL database
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

            # close cursor
            cursor.close()
            # commit the changes
            # best practice is to first execute all commands and then commit
            # reference - https://www.postgresql.org/docs/current/populate.html
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '[ERROR] ', error)

    def populatedatabase(self, connection):
        """ Populate tables in the PostgreSQL database

        Current data table data types in pandas,

            Province_State            object
            Country_Region            object
            Last_Update       datetime64[ns]
            Confirmed                  int32
            Deaths                     int32
            Recovered                  int32
            Job_id                     int64
            Latitude                 float64
            Longitude                float64
            County                    object
            Active                   float64
            Created_Date      datetime64[ns]

        References,
        1. Datatypes - https://www.postgresql.org/docs/8.1/datatype.html
        2. Best practices to populate DB - https://www.postgresql.org/docs/current/populate.html

        """
        commands = (
            """
            CREATE SCHEMA covid19data;
            """,
            """
            CREATE TABLE covid19data.jobs (
                Insert_Date TIMESTAMP,
                Job_id INT8,
                Last_File_Date TIMESTAMP
                ) PARTITION BY RANGE (Insert_Date);
            """,
            """ 
            CREATE TABLE covid19data.daily_reports (
                Job_id INT8,
                Province_State VARCHAR(255),
                Country_Region VARCHAR(255),
                Last_Update TIMESTAMP,
                Confirmed INT4,
                Deaths INT4,
                Recovered INT4,
                Created_Date TIMESTAMP,
                Latitude FLOAT8,
                Longitude FLOAT8,
                Inserted_Date TIMESTAMP
            ) PARTITION BY RANGE (Created_Date);
            """)
        self.executecommands(connection, commands)
        return 'Database was populated'

    def dropschema(self, connection):
        """
        Drop the schema if already exists PostgreSQL database
        """
        commands = (
            """
            DROP SCHEMA IF EXISTS covid19data CASCADE
            """,)
        self.executecommands(connection, commands)
        return 'Schema was dropped'
