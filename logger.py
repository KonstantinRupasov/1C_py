"""
Logging
"""
class LoggerClass:
    """
    Logging events to file or printing them out
    """
    def __init__(self, mode='2print', filename='', filemode='a'):
        """
        Object initialization
        mode:
            - '2print'
            - '2file'
        filemode:
            - 'w'	open for writing, truncating the file first
            - 'a'	open for writing, appending to the end of the file if it exists'
        """
        self.mode = mode
        if self.mode == '2file':
            self.logfile = open(filename, filemode)

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


        