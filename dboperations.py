import psycopg2

import keyoperations


class DBOperations:
    def connect2db():
        # Reading the key
        key = keyoperations.readkey()

        # Define connection
        connection = psycopg2.connect(user=key['user'],
                                      password=key['password'],
                                      host=key['host'],
                                      port=key['port'],
                                      database=key['database'])

        return connection


# Reading the key
key = keyoperations.readkey()

# Define the database connection
try:
    connection = psycopg2.connect(user=key['user'],
                                  password=key['password'],
                                  host=key['host'],
                                  port=key['port'],
                                  database=key['database'])

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print(connection.get_dsn_parameters(), "\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    # closing database connection.
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
