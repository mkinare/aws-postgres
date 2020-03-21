import pickle


def createkey():
    """
        Method to create the key to be used for connection.

        """

    key = {
        'user': 'user',
        'password': 'password',
        'host': 'host',
        'port': '5432',
        'database': 'postgres'
    }

    pickle_out = open("key.pickle", "wb")
    pickle.dump(key, pickle_out)
    pickle_out.close()


def readkey(file='key.pickle'):
    """
        Method to read the key to be used for connection.

        Returns
        -------
        dict
            Returns a dictionary object

        """

    # Reading the key
    pickle_in = open(file, "rb")
    key = pickle.load(pickle_in)
    return key

# createkey()
