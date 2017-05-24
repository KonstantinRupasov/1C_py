"""
Working with 1C:Enterprise
"""
import os
import subprocess as sub
import re
import logger as L

class OneCClass():
    """
    Class implementing all necessary functionality to work with 1C:Enterprise
    """

    def __init__(
            self,
            logger,
            version,
            path='C:\\Program Files (x86)\\1cv8\\',
            server_name='localhost'):
        """
        Params:
            - version: version of 1C:Enterprise to work with
            - path: path to 1C:Enterprise main catalog
            - server_name: name of 1C:Enterprise cluster
        """
        self.path = os.path.join(path, version, 'bin')
        os.chdir(self.path)
        self.server_name = server_name
        self._logger = logger
        #Check if ras is running. Run it if necessary
        self._logger.log(['Checking if RAS is running...'])
        res = sub.call('tasklist | findstr ras.exe', shell=True)     #Check if ras is running
        if not res == 0:                                                #Ras is not running
            self._logger.log(['RAS is not running. Starting RAS...'])
            res = sub.call('ras.exe cluster', shell=True)
            if not res == 0:
                self._logger.log(['Error starting RAS...', res])
                raise ChildProcessError('Error starting RAS', res)
            else:
                self._logger.log(['RAS is started successfully'])
        #Get cluster GUID
        res = sub.check_output('rac.exe cluster list').decode('utf-8')
        match = re.search(r'cluster\s*: ', res)
        cluster_guid_pos = match.end()
        self._cluster_guid = res[cluster_guid_pos:cluster_guid_pos+36]

    def create_infobase(self, ibname, dbms):
        """
        Create a new 1C:Enterprise infobase in cluster
        """
        command = 'rac infobase \
        --cluster={cluster} \
        create --name={name} \
        --dbms=MSSQLServer --db-server={db_server} \
        --db-user={db_user} --db-pwd={db_pwd} \
        --db-name={db_name} --locale=pl --date-offset=2000 --security-level=1 \
        --license-distribution=allow'.format(name=ibname,
                                             cluster=self._cluster_guid,
                                             db_server=dbms['db_server'],
                                             db_user=dbms['db_user'],
                                             db_pwd=dbms['db_pass'],
                                             db_name=ibname)
        self._logger.log(['About to create a new 1C infobase with command:', command])
        try:
            #os.system(command)
            res = sub.check_output(command)
            #Get infobase GUID
            infobase_guid = res[11:].decode('utf-8')            #res format is "infobase : XXXXXXXX"
            self._logger.log([res, 'New 1C infobase {} is created'.format(ibname), 'IB GUID={}'.format(infobase_guid)])
            return infobase_guid
        except Exception as exc:
            self._logger.log(['Error creating 1C IB', str(exc)])
            return ''

if __name__ == "__main__":
    LOGGER = L.LoggerClass(mode='2print')
    ONEC = OneCClass(logger=LOGGER, version='8.3.10.2252')
    print(ONEC._cluster_guid)
