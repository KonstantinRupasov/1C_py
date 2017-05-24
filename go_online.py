"""
Get the reserve server online:
    - Cycle through all "Restoring..." databases. For each:
        - Get the database online
        - Check if there is 1C ingobase for this database. Create if necessary
        - Check if there IIS publication for this database. Create if necessary
The sript assumes that all "Restoring..." databases are needed to be recovered and published
The sript assumes that no online databases are needed to be recovered and published
"""
import MSSQL
import logger as L

LOGGER = L.LoggerClass(mode='2file', path='C:\\SAAS\\LOGS\\GoOnline')
MSSQL = MSSQL.MSSQLClass(server_name='ETS', username='ETS', pwd='A3yhUv1Jk9fR', database_name='master', logger=LOGGER)
count = 0
print('Started getting databases online...')
for dbname in MSSQL.get_restoring_dbs():
    count += 1
    print('{}. Getting database {} online...'.format(count, dbname))
    MSSQL.get_db_online(dbname)
    print('{}. Database {} is online'.format(count, dbname))
print('All {} databases are online'.format(count))
