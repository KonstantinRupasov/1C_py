"""
Restore "demo" infobase from DT file
"""
import OneC
import logger as L
import credentials as cr

LOGGER = L.LoggerClass(mode='2file', path='C:\\CreateNewIB\\LOG\\RestoreDemo')
ONEC = OneC.OneCClass(logger=LOGGER, version='8.3.7.2027')
ONEC.restore_ib(ibname='demo', file_name='C:\\CreateNewIB\\demo.dt', username='root', pwd='AdVena103')
