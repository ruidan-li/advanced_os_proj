from confluent_kafka import Producer
import socket

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'client.id': socket.gethostname()}

class KafkaProducer(Producer):
    def __init__(self, config):
        super(KafkaProducer, self).__init__(config)
        self._topic = config['topic']

    def produce(self, key, val):
        self._producer.produce(self._topic, key=key, value=val, callback=self.delivery_callback)

    @staticmethod
    def delivery_callback(err, msg):
        # Optional per-message delivery callback (triggered by poll() or flush())
        # when a message has been successfully delivered or permanently
        # failed delivery (after retries).
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    def poll(self, block_sec):
        self._producer.poll(block_sec)
