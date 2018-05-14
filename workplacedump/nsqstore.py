import gnsq

from deeputil import Dummy

DUMMY_LOG = Dummy()

class NsqStore(object):
    def __init__(self, host='localhost', http_port=4151, log=DUMMY_LOG):
        self.host = host
        self.http_port = http_port
        self.log = log
        self.connection = gnsq.Nsqd(
            address=self.host, http_port=self.http_port)

    def store_in_nsq(self, record):
        self.log.debug('Storing in NSQ')
        self.connection.publish('deepcomputeworkplace', record)
        return True
