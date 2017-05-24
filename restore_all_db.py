"""
Restore all database backups from the backup catalog
Each database backup are in one subcatalog of BACKUP_PATH
Name of the subcatalog = database name
"""
import MSSQL
import logger as L
import os
import time

def dbnames_gen(backup_path):
    """
    Generator yielding database names.
    Reads all subcatalogs from backup_path
    Considers each subcatalog name to be dbname
    Doesn tell new dbs from existing ones
    """
    for dbname in os.listdir(backup_path):
        if os.path.isdir(os.path.join(backup_path, dbname)):
            yield dbname

LOG_DIR = 'C:\\SAAS\\LOGS'
LOG_FILENAME = time.strftime('%Y%m%d_%H%M%S.log')
LOGGER = L.LoggerClass(mode='2file', filename=os.path.join(LOG_DIR, LOG_FILENAME), filemode='w')
MSSQL = MSSQL.MSSQLClass(server_name='NS3075285', username='sa', pwd='HB2r8iJt', database_name='master', logger=LOGGER)
BACKUP_PATH = 'C:\\Dropbox (1C-Poland)\\BACKUPS'
count = 0
print('Started restoring databases...')
for dbname in dbnames_gen(BACKUP_PATH):
    count += 1
    print('{}. Restoring {} database...'.format(count, dbname))
    db_backup_path = os.path.join(BACKUP_PATH, dbname)
    MSSQL.restore_db(db_backup_path, dbname)
    print('{}. Database {} is restored'.format(count, dbname))
print('All {} databases are restored'.format(count))
