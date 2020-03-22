import psycopg2

import keyoperations


class DBOperations:
    def connect2db():
        # Reading the key
        key = keyoperations.readkey()

        try:
            # Define connection
            connection = psycopg2.connect(user=key['user'],
                                          password=key['password'],
                                          host=key['host'],
                                          port=key['port'],
                                          database=key['database'])

            return connection
        except (Exception, psycopg2.Error) as error:
            return 'Error while connecting to PostgreSQL', error
