""" Kafka Consumer, used throughout project for ingestion of data stored in specific kafka topics. """
from pykafka import KafkaClient

from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch as tk

__version__ = '0.1.0'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Consumer():
    """ Simple balanced kafka consumer. Accepts kafka url string and topic name byte-string. (Optional) Time in ms to stay active. """
    def __init__(self, url, topic, timeout=-1):
        """ Init requires kafka url:port, topic name, and timeout for listening.
        :param url: Kafka connection url
        :type url: string
        :param topic: Topic name of which to consume from
        :type topic: byte string
        :param timeout: Amount of time to stay listening, -1 = infinite
        :type timeout: integer
        """
        self.client = KafkaClient(hosts=url, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.consumer = self.topic.get_simple_consumer(consumer_timeout_ms=timeout)
        # self.consumer = self.topic.get_balanced_consumer(
        #     consumer_group=b'strom',
        #     num_consumer_fetchers=1,
        #     reset_offset_on_start=False,
        #     auto_commit_enable=True,
        #     auto_commit_interval_ms=30000, #tweak
        #     queued_max_messages=600, #tweak
        #     consumer_timeout_ms=timeout,
        #     auto_start=False,
        #     use_rdkafka=False,
        #     fetch_min_bytes=1, #tweak
        #     fetch_message_max_bytes=2097152, #tweak
        #     fetch_wait_max_ms=100) #tweak

    def consume(self):
        """  """
        tk['Consumer.consume : self.consumer.start'].start()
        self.consumer.start() #auto-start
        tk['Consumer.consume : self.consumer.start'].stop()
        for msg in self.consumer:
            if msg is not None:
                logger.debug(str(msg.value) + ": {}".format(msg.offset))

    def engorge(self):
        """ Consume multiple messages in queue at once and exit. """
        tk["Consumer.engorge"].start()
        tk['Consumer.engorge : self.consumer.start'].start()
        self.consumer.start()
        tk['Consumer.engorge : self.consumer.start'].stop()
        tk['Consumer.engorge : self.consumer.consume'].start()
        result = self.consumer.consume()
        tk['Consumer.engorge : self.consumer.consume'].stop()
        tk['Consumer.engorge : self.consumer.stop'].start()
        self.consumer.stop()
        tk['Consumer.engorge : self.consumer.stop'].stop()
        tk["Consumer.engorge"].stop()
        return result.value

    def stahp(self):
        """ Wrapper function for stopping Client consumer. """
        tk["Consumer.stahp"].start()
        self.consumer.stop()
        tk["Consumer.stahp"].stop()
        return True
