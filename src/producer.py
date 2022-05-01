from confluent_kafka import Producer
import socket

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'client.id': socket.gethostname()}

class KafkaProducer(Producer):
    def __init__(self, topic, config):
        super(KafkaProducer, self).__init__(config)
        self._topic = topic

    def produce(self, key, val):
        self._producer.produce(self._topic, key=key, value=val, callback=self.delivery_callback)

    @staticmethod
    def delivery_callback(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    def poll(self, block_sec):
        self._producer.poll(block_sec)
