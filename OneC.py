"""
Working with 1C:Enterprise
"""
import os
import subprocess as sub
import re
import logger as L
import credentials as cr

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
        self._logger.log(['Cluster GUID is {}'.format(self._cluster_guid)])
        #Get the list of infobases
        res = sub.check_output('rac infobase summary list --cluster={}'.format(self._cluster_guid)).decode('utf-8').split('\r\n')
        self._logger.log(['Infobases in cluster {}:'.format(self._cluster_guid)])
        self.infobases = {}
        for row in res:
            if row.startswith('infobase'):
                infobase = row[11:]
            elif row.startswith('name'):
                name = row[11:]
                self.infobases[name] = infobase
                self._logger.log(['{} : {}'.format(name, infobase)])

    def create_infobase(self, ibname, dbms, locale=''):
        """
        Create a new 1C:Enterprise infobase in cluster
        """
        #Check if the infobase already exists
        if ibname in self.infobases:
            self._logger.log(['Infobases {} is already in the cluster:'.format(ibname)])
            return self.infobases[ibname]
        #Add a new infobase
        os.chdir(self.path)
        command = 'rac infobase' + \
            ' --cluster={cluster}' + \
            ' create --name={name}' + \
            ' --dbms=MSSQLServer --db-server={db_server}' + \
            ' --db-user={db_user} --db-pwd={db_pwd}' + \
            ' --db-name={db_name} --date-offset=2000 --security-level=1' + \
            ' --license-distribution=allow'
        command = command.format(
            name=ibname,
            cluster=self._cluster_guid,
            db_server=dbms['SERVER_NAME'],
            db_user=dbms['USER_NAME'],
            db_pwd=dbms['PWD'],
            db_name=ibname)
        if locale != '':
            command = command + ' --locale={}'.format(locale)
        self._logger.log(['Creating {} infobase with command:'.format(ibname), command])
        try:
            res = sub.check_output(command)
            #Get infobase GUID
            infobase_guid = res[11:].decode('utf-8')            #res format is "infobase : XXXXXXXX"
            self._logger.log([res, 'New 1C infobase {} is created'.format(ibname), 'IB GUID={}'.format(infobase_guid)])
            return infobase_guid
        except Exception as exc:
            self._logger.log(['Error creating 1C IB', str(exc)])
            return ''

    def publish_infobase(
            self,
            ibname,
            web_server='iis',
            www_root='C:\\inetpub\\wwwroot',
            one_c_server='localhost',
            template_vrd=''):
        """
        Publish the infobase to web server
        Parameters:
            web_server: web server type:
                iis: MS IIS
                apache2: Apache 2.0
                apache22: Apache 2.2
                apache24: Apache 2.4
        """
        os.chdir(self.path)
        _dir = os.path.join(www_root, ibname)
        command = 'webinst -publish -{web_server} -wsdir {ibname} -dir {_dir}' + \
            ' -connstr Srvr={one_c_server};Ref={ibname}'
        command = command.format(
            ibname=ibname,
            web_server=web_server,
            _dir=_dir,
            one_c_server=one_c_server)
        if template_vrd != '':
            command = command + ' -descriptor {template_vrd}'
            command = command.format(template_vrd=template_vrd)
        self._logger.log(['Publishing {} infobase with command:'.format(ibname), command])
        try:
            res = sub.check_output(command, shell=True)
            #Get infobase GUID
            self._logger.log([res.decode('utf-8'), 'Infobase {} is published to web sucessfully'.format(ibname)])
        except Exception as exc:
            self._logger.log(['Error publishing infobase {}'.format(ibname), str(exc)])

if __name__ == "__main__":
    LOGGER = L.LoggerClass(mode='2print')
    ONEC = OneCClass(logger=LOGGER, version='8.3.10.2252')
    ONEC.create_infobase('DR_IT', cr.DBMS, locale='pl')
    ONEC.publish_infobase(ibname='DR_IT', template_vrd='C:\\SAAS\\default.vrd')
