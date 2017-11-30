""" Kafka Consumer """
from pykafka import KafkaClient
import pykafka.utils.compression as Compression

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Consumer():
    """ Simple balanced kafka consumer. """
    def __init__(self, url, topic, zk_url):
        """ Init requires kafka url:port, topic name, and zookeeper url:port. """
        self.client = KafkaClient(hosts=url, zookeeper_hosts=None, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=b'test',
            num_consumer_fetchers=1,
            reset_offset_on_start=False,
            zookeeper_connect=zk_url,
            auto_commit_enable=True,
            auto_commit_interval_ms=60000,
            queued_max_messages=2000,
            consumer_timeout_ms=1,
            auto_start=True,
            use_rdkafka=False)  # NOTE: may be quicker w/ alt. options

    def _snappy_decompress(self, msg):
        msg_unpkg = Compression.decode_snappy(msg)
        return msg_unpkg
    def _gzip_decompress(self, msg):
        msg_unpkg = Compression.decode_gzip(msg)
        return msg_unpkg
    def _lz4_decompress(self, msg):
        msg_unpkg = Compression.decode_lz4_old_kafka(msg)
        return msg_unpkg

    def listen(self):
        """ Actively 'listen' on given topic. Check consumer_timeout_ms option in init. """
        # self.consumer.start() #auto-start
        for msg in self.consumer:
            if msg is not None:
                print(msg.value)#NOTE: TEMP
                #pass message on to keep listening

    def consume(self):
        """ Collect all new messages in a topic at once. """
        # self.consumer.start() #auto-start
        msg = self.consumer.consume()
        if msg is not None:
            return msg
