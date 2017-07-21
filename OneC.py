"""
Working with 1C:Enterprise
"""
import os
import subprocess as sub
from psutil import process_iter as ps
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
        if 'ras.exe' not in [p.name() for p in ps()]:
            #Ras is not found. Run it now
            self._run_command('RAS is not running. Starting RAS', 'ras.exe cluster', service=True)
        #Get cluster GUID
        output = self._run_command('Getting the cluster GUID:', 'rac.exe cluster list')
        for row in output:
            if row.startswith('cluster'):
                self._cluster_guid = row[32:]
        self._logger.log(['Cluster GUID is {}'.format(self._cluster_guid)])
        #Get the list of infobases
        output = self._run_command('Getting the list of infobases', 'rac infobase summary list --cluster={}'.format(self._cluster_guid))
        self._logger.log(['Infobases in cluster {}:'.format(self._cluster_guid)])
        self.infobases = {}
        for row in output:
            if row.startswith('infobase'):
                infobase = row[11:]
            elif row.startswith('name'):
                name = row[11:]
                self.infobases[name] = infobase
                self._logger.log(['{}: {}'.format(name, infobase)])

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
        output = self._run_command('Creating {} infobase:'.format(ibname), command)
        infobase_guid = output[11:].decode('utf-8')            #res format is "infobase : XXXXXXXX"
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
            
    def disconnect_ib_users(self, ibname, username='', pwd=''):
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
        command = self._add_user_credentials(command, 'rac', username, pwd)
        output = self._run_command('Getting the list of {} infobase connections:'.format(ibname), command)
        self._logger.log(['List of {} infobase connections:'.format(ibname)])
        for row in output:
            self._logger.log([row])
        #Cycle through the list of connections and close them
        for row in output:
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
                command = self._add_user_credentials(command, 'rac', username, pwd)
                self._run_command('Closing a connection:', command)

    def set_new_sessions_lock(self, ibname, mode='on', username='', pwd=''):
        """
        Set ibnfobase new session lock mode to on or off
        """
        self._check_value('mode parameter', mode, ('on', 'off'))
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
        command = self._add_user_credentials(command, 'rac', username, pwd)
        self._run_command('Setting session lock mode to {}'.format(mode), command)

    def restore_ib(self, ibname, file_name, username='', pwd=''):
        """
        Restore the infobase from DT file
        """
        #Lock new sessions
        self.set_new_sessions_lock(ibname, mode='on', username=username, pwd=pwd)
        #Disconnect all the users from the infobase
        self.disconnect_ib_users(ibname, username=username, pwd=pwd)
        #Restore the infobase
        command = '{path}\\1cv8.exe DESIGNER' + \
            ' /S localhost\{ibname} /RestoreIB "{file_name}"' + \
            ' /DisableStartupMessages /DisableStartupDialogs'
        command = command.format(
            path=self.path,
            ibname=ibname,
            file_name=file_name
        )
        command = self._add_user_credentials(command, '1cv8', username, pwd)
        #Unlock new sessions
        self.set_new_sessions_lock(ibname, mode='off', username=username, pwd=pwd)
        #Restore IB
        self._run_command(
            'Restoring {} infobase from DT file'.format(ibname),
            command)

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
    
    def _run_command(self, descr, command, service=False):
        """
        Run the command using sub.check_call
        Returns the OS result of the command execution
        Puts the output to self.tmp_file, reads it and returns back to caller
        if service == True:
            Do not wait until the command is executed
        """
        self._logger.log([descr, command])
        output = ''
        try:
            proc = sub.Popen(command, stdout=sub.PIPE, stderr=sub.PIPE)
            if not service:
                proc.wait()
                err = proc.stderr.read().decode('utf-8')
                if err != '':
                    #Error during command execution
                    raise ChildProcessError('Error {} when {}'.format(err, descr))
                else:
                    output = proc.stdout.read().decode('utf-8')
            self._logger.log(['Success'])
            return output.split('\r\n')
        except Exception as exc:
            self._logger.log(['Error:', str(exc)])
            raise exc

    def _check_value(self, name, value, valid_values):
        """
        Checks if the value is in the valid_values list
        Raises exception if it's not
        """
        if value not in valid_values:
            self._logger.log(['Invalid {} value. Valid values:'.format(name), valid_values])
            raise ValueError('Invalid {} value. Valid values: {}'.format(name, valid_values))

    def _add_user_credentials(self, command, tool='rac', username='', pwd=''):
        """
        Add username and wd to command (if they are specified)
        Uses parameter names according to the tool syntax. 
        Valid tools:
            - rac
            - 1cv8
        """
        if username != '':
            command = command + ' {param}"{value}"'
            if tool == 'rac': 
                param='--infobase-user='
            elif tool == '1cv8':
                param='/N '
            command = command.format(param=param, value=username)
        if pwd != '':
            command = command + ' {param}"{value}"'
            if tool == 'rac': 
                param='--infobase-pwd='
            elif tool == '1cv8':
                param='/P '
            command = command.format(param=param, value=pwd)
        return command
        
if __name__ == "__main__":
    LOGGER = L.LoggerClass(mode='2print')
    ONEC = OneCClass(logger=LOGGER, version='8.3.10.2252')
    ONEC.restore_ib(ibname='PMC_ACS', file_name='D:\\Rupasov\\TMP\\1cv8.dt', username='admin', pwd='admin')
    #ONEC.create_infobase('DR_IT', cr.DBMS, locale='pl')
    #ONEC.publish_infobase(ibname='DR_IT', template_vrd='C:\\SAAS\\default.vrd')
