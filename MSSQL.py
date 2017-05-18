"""
Working with MS SQL Server
"""
import pyodbc as odbc
import logger as L
import os

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

    def create_db_by_attaching_files(self, dbname, templatename='', files=[]):
        """
        Creates a new database by attaching files
        Parameters:
            - dbname: The name of the new database to be created by attaching files
            - templatename: The name of the template database
                The database has to be detached before running this procedure
                You don't need to specify the templatename if you use files array
            - files: files to be copyed and attached as the new database's files
                An array of strings
                Every string specifies the fulle specified name of the database file
                Optional parameter. If omitted the db is created by attaching these two files (default MS SQL settings):
                    - <defaul MS SQL data path>\<dbname>.mdf
                    - <defaul MS SQL data path>\<dbname>_Log.ldf
                You should not specify this parameter if you use templatename
        """
        #If files[] parameter is omitted, fill it out with the default values
        if files == []:
            if templatename == '':
                self._logger.log(['--- ERROR --- ',
                                  'MSSQLClass.create_db_by_attaching_files',
                                  'Nothing to append',
                                  'You need to specify either templatename of files array'],
                                 ValueError)
                return
            files.append('{data_path}{templatename}.mdf'.format(data_path=self._data_path, templatename=templatename))
            files.append('{data_path}{templatename}_Log.ldf'.format(data_path=self._data_path, templatename=templatename))
        sql_str = "CREATE DATABASE ""{}"" ON ".format(dbname)
        first_file_name = True
        for file_name in files:
            if not first_file_name:
                sql_str = sql_str + ', '
            first_file_name = False
            sql_str = sql_str + "(FILENAME = '{file_name}')".format(file_name=file_name)
        sql_str = sql_str + ' FOR ATTACH'
        self._logger.log(
            ['-------------------------------------',
             'About to execute this TSQL statement to attach the new IB database:',
             sql_str])
        self.cursor.execute(sql_str)
        self._logger.log(['New IB {} has been attached sucessfully'.format(dbname)])

    def backup_db_full(self, backup_filename, dbname):
        """
        Create a full db backup in self.backup_path\\dbname catalog
        """
        sql_str = "BACKUP DATABASE [{dbname}] TO  DISK = N'{backup_filename}' \
 WITH  RETAINDAYS = 1, NOFORMAT, NOINIT, NAME = N'first_full_backup', \
 SKIP, REWIND, NOUNLOAD, STATS = 10".format(dbname=dbname, backup_filename=backup_filename)
        self._logger.log(['-------------------------------------',
                          'About to create the full backup of the database:',
                          sql_str])
        #Check is the backup catalog exists. Create is necessary
        backup_path = backup_filename[0:backup_filename.rfind('\\')]
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)
        #Create the backup
        self._logger.log(['Backing up database {}...'.format(dbname)])
        self.cursor.execute(sql_str)
        while self.cursor.nextset():
            pass
        self._logger.log(['Full backup of database {} has been created sucessfully'.format(dbname)])

    def restore_db(self, backup_path, dbname, backup_ext={'full': 'bak', 'diff': 'dif', 'tlog': 'trn'}):
        """
        Restore all backups from catalog backup_path\dbname to database dbname:
            - Find latest full backup (*.backup_ext['full']) and restore it:
                - If the database doesn't exist - create it (was added to the PROD server)
                - Delete all full backups
            - Find latest diff backup (*.backup_ext['diff']) and restore it
            - Delete all diff backups
            - Restore all transaction log backups (*.backup_ext['trn'])
            - Delete all transaction log backups
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
            self._exec_sql(sql_str, 'Restoring {dbname} from {file}:'.format(dbname=dbname, file=file))
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
    #MSSQL.create_db_by_attaching_files(dbname='asd1', templatename='template')
    #print(MSSQL)
    #MSSQL.backup_db_full('C:\\Program Files\\Microsoft SQL Server\\MSSQL12.MSSQLSERVER\\MSSQL\\Backup\\DR_IT\\DR_IT2.bak', 'DR_IT')
    MSSQL.restore_db('D:\\Rupasov\\1C-Polland\\BACKUP\\prissystem', 'prissystem')
