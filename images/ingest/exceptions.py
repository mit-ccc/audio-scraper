'''
Audio ingest exceptions
'''


class IngestException(Exception):
    '''Base class for audio ingest exceptions'''


class JobCancelledException(IngestException):
    '''A running job has been cancelled'''


class TooManyFailuresException(IngestException):
    '''A running job has failed too many times'''
