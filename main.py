import dbmethods
import processdata

DB = dbmethods.DBOperations()
connection = DB.connect2db()
cursor = connection.cursor()
# Drop the schema for the database
DB.dropschema(connection)
# Populate the database
DB.populatedatabase(connection)
# Add data
df = processdata.mergefiles()
DB.copy2dbfromdf(connection=connection, df=df, tblname='covid19data.daily_reports')
for i in list(range(9)):
    DB.copy2dbfromdf(connection=connection, df=df.sample(3), tblname='covid19data.daily_reports')

cursor.close()
connection.close()
print("PostgreSQL connection is closed")
