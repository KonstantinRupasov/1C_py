"""
Logging
"""
import time

class LoggerClass:
    """
    Logging events to file or printing them out
    Always creates a new file
    """
    def __init__(self, mode='2print', path=''):
        """
        Object initialization
        mode:
            - '2print'
            - '2file'
        """
        self.mode = mode
        if self.mode == '2file':
            self.path = path
            self.filename = time.strftime('%Y%m%d_%H%M%S.log')
            self.logfile = open(self.filename, 'w')

    def log(self, messages, exception=None):
        """
        Log the messages
        messages - array of strings
        """
        for message in messages:
            if self.mode == '2file':
                print(message, file=self.logfile)
            else:
                print(message)
        if self.mode == '2file':
            self.logfile.flush()
        if exception is not None:
            raise exception(messages)


        