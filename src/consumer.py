from confluent_kafka import Consumer, KafkaError, KafkaException

# conf = {'bootstrap.servers': "host1:9092,host2:9092",
#         'group.id': "foo",
#         'auto.offset.reset': 'smallest'}

class KafkaConsumer(Consumer):
    def __init__(self, topic, config):
        super(KafkaConsumer, self).__init__(config)
        self._topic = topic
        self._running = False
    
    def start(self):
        self._running = True

    def stop(self):
        self._running = False

    def basic_consume(self, timeout):
        try:
            self.subscribe(self._topic)

            while self._running:
                msg = self.poll(timeout=timeout)
                if msg is None: continue    # TODO: might want to track how many none are returned in case we want to log sth

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    self.msg_process(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.close()

    def msg_process(self, msg):
        pass