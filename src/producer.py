from confluent_kafka import Producer, KafkaException
import socket
import random

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'client.id': socket.gethostname()}

class KafkaProducer(Producer):
    def __init__(self, config, topic, num_pa):
        super(KafkaProducer, self).__init__(config)
        self._topic = topic
        self._num_pa = int(num_pa)
        self._produce_track = [0 for _ in range(self._num_pa)]

    def perform_produce(self, key=None, val=None):
        # self.produce(self._topic, key=key, value=val, callback=self.delivery_callback)
        try:
            self.produce(self._topic, key=None, value=val, partition=int(key) % self._num_pa, callback=self.delivery_callback)
            self._produce_track[int(key) % self._num_pa] += 1
        except KafkaException as e:
            print(f'producing to {key % self._num_pa}. exception: {e}')
        except BufferError:
            self.flush()

    def get_stats(self):
        return self._produce_track

    @staticmethod
    def delivery_callback(err, msg):
        # Optional per-message delivery callback (triggered by poll() or flush())
        # when a message has been successfully delivered or permanently
        # failed delivery (after retries).
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            if random.randint(0, 100) == 1:
                if msg.key():
                    print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                else:
                    print("Produced event to topic {topic}: key = None value = {value:12}".format(
                        topic=msg.topic(), value=msg.value().decode('utf-8')))
