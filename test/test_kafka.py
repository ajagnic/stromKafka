import unittest

from strom.kafka.consumer.consumer import Consumer
from strom.kafka.producer.producer import Producer


class TestKafka(unittest.TestCase):
    def setUp(self):
        self.producer = Producer('127.0.0.1:9092', b'kafkatest')
        self.consumer = Consumer('127.0.0.1:9092', b'kafkatest',timeout=10000)

    def test_kafka_init(self):
        self.assertIsInstance(self.producer, Producer)
        self.assertIsInstance(self.consumer, Consumer)

    def test_kafka_pipeline(self):
        message = "Test Message !@%&!"
        message_encode = message.encode()
        self.producer.produce(message_encode)
        consumed_message = self.consumer.engorge()
        consumed_message_decode = consumed_message.decode("utf-8")
        self.assertEqual(consumed_message_decode, "Test Message !@%&!")

if __name__ == '__main__':
    unittest.main()
