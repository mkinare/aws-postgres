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
DB.populatedatabase(connection)

cursor.close()
connection.close()
print("PostgreSQL connection is closed")
