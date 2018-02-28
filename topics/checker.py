from pykafka import KafkaClient

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class TopicChecker():
    """ Class that implements callback functions for current or new topics. """
    def __init__(self, url):
        self.client = KafkaClient(hosts=url)
        self.topics = self.client.topics

    def _update(self):
        self.client.update_cluster()
        self.topics = self.client.topics

    def list(self):
        self._update()
        return self.topics

    def _get_len(self):
        res = self.list()
        return len(res)

    #def _check_start(self, callback):
    #    self.check(callback)

    #def check(self, keep=False, callback=None):
    #    count = len(self.topics)
    #    counting = count
    #    while count == counting:
    #        counting = self._get_len()
    #    if callback is not None:
    #        callback()
    #    if keep:
    #        self._check_start(callback)
    #    else:
    #        return (counting - count)
