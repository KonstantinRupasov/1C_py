import pyodbc as odbc

class MSSQL_class:
    """
    Class to work with MS SQL Server
    Uses pyodbc lib
    """
    def __init__(self, server_name, username, pwd, database_name='master'):
        """
        Class initialization
        """
        self.server_name = server_name
        self.database_name = database_name
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

    def create_db_by_attaching_files(self, dbname, files=[]):
        """
        Creates a new database by attaching files
        Parameters:
            - dbname: The nane of the database to create
            - files:
                - An array of strings
                - Every string specifies the full file name
                - Optional parameter. If omitted the db is created by attaching files:
                    - <defaul MS SQL data path>\<dbname>.mdf
                    - <defaul MS SQL data path>\<dbname>.ldf
        """
        pass

"""-----------------------------------------------------------
Testing
------------------------------------------------------------"""
if __name__ == "__main__":
    MSSQL = MSSQL_class('ETS', 'ETS', 'A3yhUv1Jk9fR', 'DR_IT')
    print(MSSQL)