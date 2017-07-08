"""
Working with 1C:Enterprise
"""
import os, tempfile, re
import subprocess as sub
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
        #res = sub.check_output('rac.exe cluster list').decode('utf-8')
        res = self._run_command('Getting the cluster GUID:', 'rac.exe cluster list')
        for row in res:
            if row.startswith('cluster'):
                self._cluster_guid = row[32:]        
        self._logger.log(['Cluster GUID is {}'.format(self._cluster_guid)])
        #Get the list of infobases
        #res = sub.check_output('rac infobase summary list --cluster={}'.format(self._cluster_guid)).decode('utf-8').split('\r\n')
        res = self._run_command('Getting the list of infobases', 'rac infobase summary list --cluster={}'.format(self._cluster_guid))
        self._logger.log(['Infobases in cluster {}:'.format(self._cluster_guid)])
        self.infobases = {}
        for row in res:
            if row.startswith('infobase'):
                infobase = row[11:]
            elif row.startswith('name'):
                name = row[11:]
                self.infobases[name] = infobase
                #self._logger.log(['{} : {}'.format(name, infobase)])

    def create_infobase(self, ibname, dbms, locale=''):
        """
        Create a new 1C:Enterprise infobase in cluster
        """
        #Check if the infobase already exists
        if ibname in self.infobases:
            self._logger.log(['Infobases {} is already in the cluster:'.format(ibname)])
            return self.infobases[ibname]
        #Add a new infobase
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
        res = self._run_command('Creating {} infobase:'.format(ibname), command)
        #self._logger.log(['Creating {} infobase with command:'.format(ibname), command])
        #try:
        #    res = sub.check_output(command)
        #    #Get infobase GUID
        #except Exception as exc:
        #    self._logger.log(['Error creating 1C IB', str(exc)])
        #    raise ChildProcessError('Error creating 1C IB', str(exc))
        infobase_guid = res[11:].decode('utf-8')            #res format is "infobase : XXXXXXXX"
        #self._logger.log([res, 'New 1C infobase {} is created'.format(ibname), 'IB GUID={}'.format(infobase_guid)])
        return infobase_guid

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
        self._run_command('Publishing {} infobase:'.format(ibname), command)
        #self._logger.log(['Publishing {} infobase with command:'.format(ibname), command])
        #try:
        #    res = sub.check_output(command, shell=True)
            #Get infobase GUID
        #except Exception as exc:
        #    self._logger.log(['Error publishing infobase {}'.format(ibname), str(exc)])
        #    raise ChildProcessError('Error publishing infobase {}'.format(ibname), str(exc))
        #self._logger.log([res, 'Infobase {} is published to web sucessfully'.format(ibname)])
            
    def disconnect_ib_users(self, ibname):
        """
        Disconnect all infobase users
        """
        ib_guid = self._get_ib_guid(ibname)
        if ib_guid == None:
            return
        #Get the list of infobase connections
        command = 'rac connection list ' + \
            '--cluster={cluster_guid} ' + \
            '--infobase={ib_guid}'
        command = command.format(
            cluster_guid=self._cluster_guid,
            ib_guid=ib_guid
        )
        res = self._run_command('Getting the list of {} infobase connections:'.format(ibname), command)
        #self._logger.log(['Getting the list of {} infobase connections:'.format(ibname), command])
        #try:
        #    res = sub.check_output(command).decode('utf-8').split('\r\n')
        #except Exception as exc:
        #    self._logger.log(['Error getting the list if IB connections', str(exc)])
        #    raise ChildProcessError('Error getting the list if IB connections', str(exc))
        #Cycle through the list of connections and close them
        for row in res:
            if row.startswith('connection'):
                connection_guid = row[17:]
            elif row.startswith('process'):
                process_guid = row[17:]
                command = 'rac connection disconnect' + \
                        ' --cluster={cluster_guid}' + \
                        ' --process={process_guid}' + \
                        ' --connection={connection_guid}'
                command = command.format(
                    cluster_guid=self._cluster_guid,
                    process_guid=process_guid,
                    connection_guid=connection_guid
                )
                self._run_command('Closing a connection:', command)
                #self._logger.log(['Closing a connection:', command])
                #Close the connection
                #try:
                #    res = sub.check_output(command).decode('utf-8').split('\r\n')
                #except Exception as exc:
                #    self._logger.log(['Error closing connection', str(exc)])
                #    raise ChildProcessError('Error closing connection', str(exc))
                #self._logger.log(['Connection is closed'])

    def set_new_sessions_lock(self, ibname, mode='on'):
        """
        Set ibnfobase new session lock mode to on or off
        """
        if mode not in ('on', 'off'):
            raise ValueError('Invalid mode parameter value. Valid values: "on", "off"')
        ib_guid = self._get_ib_guid(ibname)
        command = 'rac infobase update' + \
            ' --cluster={cluster_guid}' + \
            ' --infobase={infobase_guid}' + \
            ' --sessions-deny={mode}'
        command = command.format(
            cluster_guid=self._cluster_guid,
            infobase_guid=ib_guid,
            mode=mode            
        )
    def restore_ib(self, ibname, file_name):
        """
        Restore the infobase from DT file
        """
        #Lock new sessions
        self.set_new_sessions_lock(ibname, mode='on')
        #Disconnect all the users from the infobase
        self.disconnect_ib_users(ibname)
        #Create a temp file to store 1cv8 system log
        log_file, log_filename = tempfile.mkstemp()
        os.close(log_file)
        #Restore the infobase
        command = '{path}\\1cv8.exe DESIGNER' + \
            ' /S localhost\{ibname} /RestoreIB "{file_name}"' + \
            ' /Out "{log_filename}"'
        command = command.format(
            path=self.path,
            ibname=ibname,
            file_name=file_name,
            log_filename=log_filename
        )
        try:
            res = sub.check_output(command).decode('utf-8').split('\r\n')
        except Exception as exc:
            self._logger.log(['Error restoring infobase', str(exc)])
            #Read the log_file
            log_file = open(log_filename)
            self._logger.log([row for row in log_file])
            #Unlock new sessions
            self.set_new_sessions_lock(ibname, mode='on')
            raise ChildProcessError('Error restoring infobase', str(exc))
        #Check log_file. Should be empty if everything's OK
        self._logger.log(['Infobase is restored successfully'])

    def _get_ib_guid(self, ibname):
        """
        Find the IB with given name in the cluster
        Returns IB GUID
        """
        try:
            ib_guid = self.infobases[ibname]
        except KeyError:
            self._logger.log(['Cannot find infobase {}'.format(ibname)])
            ib_guid = None
            raise KeyError('Cannot find infobase {}'.format(ibname))
        return ib_guid
    
    def _run_command(self, descr, command):
        """
        Run the command using sub.check_output
        Catch exceptions
        """
        self._logger.log([descr, command])
        try:
            res = sub.check_output(command).decode('utf-8').split('\r\n')
        except Exception as exc:
            self._logger.log(['Error:', str(exc)])
            #Read the log_file
            raise ChildProcessError('Error: ', str(exc))
        #Check log_file. Should be empty if everything's OK
        self._logger.log(['Success'])
        return(res)
        
if __name__ == "__main__":
    LOGGER = L.LoggerClass(mode='2print')
    ONEC = OneCClass(logger=LOGGER, version='8.3.10.2252')
    ONEC.restore_ib('PMC_ACS', 'C:\\Users\\Rupasov\\Downloads\\1cv8.dt')
    #ONEC.create_infobase('DR_IT', cr.DBMS, locale='pl')
    #ONEC.publish_infobase(ibname='DR_IT', template_vrd='C:\\SAAS\\default.vrd')
