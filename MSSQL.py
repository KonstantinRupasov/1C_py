"""
Working with MS SQL Server
"""
import os
import shutil
import pyodbc as odbc
import logger as L

class MSSQLClass:
    """
    Class to work with MS SQL Server
    Uses pyodbc lib
    """
    def __init__(
            self,
            server_name,
            username,
            pwd,
            logger,
            database_name='master'):
        """
        Class initialization
        """
        self.server_name = server_name
        self.database_name = database_name
        self.backup_path = None
        self._logger = logger
        #Connect to MS SQL
        connection_str = 'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database_name};\
        UID={username};PWD={pwd}'.format(
            server_name=server_name,
            username=username,
            pwd=pwd,
            database_name=database_name)
        connection = odbc.connect(connection_str)
        connection.autocommit = True
        self.cursor = connection.cursor()
        #Read the default MS SQL DATA path
        self.cursor.execute("select cast(serverproperty('InstanceDefaultDataPath') as varchar(255))")
        self._data_path = self.cursor.fetchall()[0][0]

    def dbnames_gen(self, backup_path):
        """
        Generator yielding database names.
        Reads all subcatalogs from backup_path
        Considers each subcatalog name to be dbname
        Doesn tell new dbs from existing ones
        ------------------------------------
        ToDo: Implement distributed message queue instead (http://www.celeryproject.org/)
        Live server should send messages to reserve, notifying it about added and deleted databases
        ------------------------------------
        """
        for dbname in os.listdir(backup_path):
            if os.path.isdir(os.path.join(backup_path, dbname)):
                yield dbname


    def create_db_by_attaching_files(self, dbname, template_dbname):
        """
        Creates a new database by copying and attaching template db files
        Parameters:
            - dbname: The name of the new database to be created by attaching files
            - template_dbname: The name of the template database
                - The database has to be detached before running this procedure
                - You don't need to specify the templatename if you use files array
                - The template dadabase has to consist of two files:
                    - {self._data_path}{template_dbname}.mdf
                    - {self._data_path}{template_dbname}_Log.ldf

        """
        #Copying template files into the ne db files
        self._logger.log(['Copying {template_dbname} database files into \
{dbname} database files'.format(template_dbname=template_dbname, dbname=dbname)])
        shutil.copy('{data_path}{template_dbname}.mdf'.format(data_path=self._data_path, template_dbname=template_dbname),
                    '{data_path}{dbname}.mdf'.format(data_path=self._data_path, dbname=dbname))
        shutil.copy('{data_path}{template_dbname}_Log.ldf'.format(data_path=self._data_path, template_dbname=template_dbname),
                    '{data_path}{dbname}_Log.ldf'.format(data_path=self._data_path, dbname=dbname))
        sql_str = "CREATE DATABASE ""{}"" ON ".format(dbname)
        self._logger.log(['Files are copied successfully'])
        sql_str = "CREATE DATABASE \"{dbname}\" ON (FILENAME = '{data_path}{dbname}.mdf'), \
(FILENAME = '{data_path}{dbname}_Log.ldf') FOR ATTACH".format(dbname=dbname, data_path=self._data_path)
        self._exec_sql(sql_str, 'Attaching {dbname} IB...'.format(dbname=dbname))

    def backup_db_full(self, backup_path, dbname):
        """
        Create a full backup of db dbname in {backup_path}\k\_{dbname}.bak file
        """
        #Check is the backup catalog exists. Create is necessary
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)
        backup_filename = os.path.join(backup_path, dbname, '_{dbname}.bak').format(dbname=dbname)
        sql_str = "BACKUP DATABASE [{dbname}] TO  DISK = N'{backup_filename}' \
 WITH  RETAINDAYS = 1, NOFORMAT, NOINIT, NAME = N'_{dbname}.bak', \
 SKIP, REWIND, NOUNLOAD, STATS = 10".format(dbname=dbname, backup_filename=backup_filename)
        #Create the backup
        self._exec_sql(sql_str, 'Creating a full backup {backup_filename} \
of database {dbname}...'.format(dbname=dbname, backup_filename=backup_filename))

    def restore_db(self, backup_path, dbname, backup_ext={'full': 'bak', 'diff': 'dif', 'tlog': 'trn'}):
        """
        Restore a single database dbname from all backup files in the catalog backup_path\dbname:
            - Find latest full backup (*.backup_ext['full']) and restore it:
                - If the database doesn't exist - create it
                - Delete all full backups
            - Find latest diff backup (*.backup_ext['diff']) and restore it. Delete all diff backups
            - Restore all transaction log backups (*.backup_ext['trn']). Delete all transaction log backups
        """
        self._logger.log(['Restoring database {dbname} from backups in {backup_path} catalog'.format(dbname=dbname, backup_path=backup_path)])
        ext = tuple(item for item in backup_ext.values())
        self._logger.log(['File extensions to look for is:'] + list(ext))
        files = [os.path.join(backup_path, file)
                 for file in os.listdir(backup_path)
                 if os.path.isfile(os.path.join(backup_path, file))
                 and file.endswith(ext)
                ]
        if files == []:
            self._logger.log(['Catalog {backup_path} contains no backup files'.format(backup_path=backup_path)])
            return
        self._logger.log(['The following files are found in {backup_path}:'.format(backup_path=backup_path)] + files)
        files2restore = []          #List of files to restore backup from
        files2delete = []           #List of files to delete (old backups, replaced by more recent files)
        del_ext = set([])           #List of extensions that we don't need anymore
        #Cycle trough files in reverse order
        files.sort(reverse=True)
        for file in files:
            if file.endswith(tuple(del_ext)):
                #We don't need this type of backup (it's replaced with more recent backups)
                files2delete.append(file)
            else:
                #We need to restore from this file
                files2restore.append(file)
                if file.endswith(backup_ext['diff']):
                    #Diff backup if found
                    #Don't need earlier TLog or Diff backups
                    del_ext.add(backup_ext['diff'])
                    del_ext.add(backup_ext['tlog'])
                if file.endswith(backup_ext['full']):
                    #Full backup if found
                    #Don't need any earlier backup
                    del_ext.add(backup_ext['full'])
                    del_ext.add(backup_ext['diff'])
                    del_ext.add(backup_ext['tlog'])
        files2delete.sort()
        self._logger.log(['The following files are selected to be deleted:'] + files2delete)
        files2restore.sort()
        self._logger.log(['The following files are selected to be restored:'] + files2restore)
        self._logger.log(['Restoring backup files...'])
        #Restore all selected files
        for file in files2restore:
            #Resore the file
            sql_str = "RESTORE DATABASE [{dbname}]\
 FROM DISK = N'{file}'  WITH FILE = 1,\
 MOVE N'{dbname}' TO N'{data_path}{dbname}.mdf',\
 MOVE N'{dbname}_log' TO N'{data_path}{dbname}_log.ldf',\
 NOUNLOAD, REPLACE, NORECOVERY, STATS = 5".format(dbname=dbname, file=file, data_path=self._data_path)
            self._exec_sql(sql_str, 'Restoring {dbname} from {file}...'.format(dbname=dbname, file=file))
            #Add the file to files2delete
            files2delete.append(file)
        #Delete all backup files
        self._logger.log(['Starting to delete unnecessary (old or already restored) backup files'])
        for file in files2delete:
            self._logger.log(['Deleting file {file}'.format(file=file)])
            os.remove(file)
            self._logger.log(['File {file} deleted'.format(file=file)])

    def _exec_sql(self, sql_str, comment):
        """
        Run TSQL query
        Wait until it is executed
        """
        self._logger.log([comment, 'About to run this SQL statement:', sql_str])
        self.cursor.execute(sql_str)
        while self.cursor.nextset():
            pass
        self._logger.log(['SQL statement successfully executed'])

"""-----------------------------------------------------------
Testing
------------------------------------------------------------"""
#LOGGER = L.LoggerClass(mode='2print')
LOGGER = L.LoggerClass(mode='2file', filename='D:\\Rupasov\\log.txt', filemode='w')
if __name__ == "__main__":
    MSSQL = MSSQLClass(server_name='ETS', username='ETS', pwd='A3yhUv1Jk9fR', database_name='master', logger=LOGGER)
    #MSSQL.create_db_by_attaching_files(dbname='asd1', template_dbname='template')
    #print(MSSQL)
    #SSQL.backup_db_full('D:\\Rupasov\\1C-Polland\\BACKUP', 'asd1')
    #MSSQL.restore_db('D:\\Rupasov\\1C-Polland\\BACKUP\\prissystem', 'prissystem')
    #for dbname in MSSQL.dbnames_gen('D:\\Rupasov\\1C-Polland\\BACKUP'):
    #    print(dbname)
    BACKUP_PATH = 'D:\\Rupasov\\1C-Polland\\BACKUP'
    for dbname in MSSQL.dbnames_gen(BACKUP_PATH):
        db_backup_path = os.path.join(BACKUP_PATH, dbname)
        MSSQL.restore_db(db_backup_path, dbname)
