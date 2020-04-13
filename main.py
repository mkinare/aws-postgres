import dboperations

DB = dboperations.DBOperations()
connection = DB.connect2db()
cursor = connection.cursor()
# Print PostgreSQL Connection properties
print(connection.get_dsn_parameters(), "\n")

# Print PostgreSQL version
cursor.execute("SELECT version();")
record = cursor.fetchone()
print("You are connected to - ", record, "\n")
# Populate the database
DB.populatedatabase(connection)
# Drop the schema for the database
DB.dropschema(connection)
print("PostgreSQL connection is closed")
#cursor.close()
#connection.close()