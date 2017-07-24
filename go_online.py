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
import OneC
import logger as L
import credentials as cr

LOGGER = L.LoggerClass(mode='2file', path='C:\\SAAS\\LOGS\\GoOnline')
MSSQL = MSSQL.MSSQLClass(cr.DBMS,
                         database_name='master',
                         logger=LOGGER)
ONEC = OneC.OneCClass(logger=LOGGER, version='8.3.7.2027')
print('Started restoring...')
dbnames = MSSQL.get_restoring_dbs()
for dbname in dbnames:
    count += 1
    print('{}.1. Getting database {} recovered...'.format(count, dbname))
    MSSQL.get_db_online(dbname)
    print('{}.1. Database {} is recovered'.format(count, dbname))
    print('{}.2. Creating 1C Infobase {}...'.format(count, dbname))
    ONEC.create_infobase(dbname, cr.DBMS, locale='pl')
    print('{}.2. 1C infobase {} is created'.format(count, dbname))
    print('{}.3. Publishing 1C infobase {} to web...'.format(count, dbname))
    ONEC.publish_infobase(ibname=dbname, template_vrd='C:\\SAAS\\default.vrd')
    print('{}.3. 1C Infobase {} is published to web'.format(count, dbname))
print('All {} databases are online'.format(count))
