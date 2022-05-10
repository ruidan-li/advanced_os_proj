# advanced_os_proj
Course project for advanced OS. Kafka pipeline

https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html

### Setup
```
$ source setup.sh 
```

### Example
```
$ cd src 
$ ./produce_driver.py basic.ini 5 10 
{'bootstrap.servers': '129.114.108.39:9092,129.114.109.13:9092,129.114.108.242:9092', 'topic': 'test'}
Produced event to topic test: key = 0            value = {"value": "v_0", "timestamp": 1652162956.099297}
Produced event to topic test: key = 6            value = {"value": "v_6", "timestamp": 1652162956.100889}
Produced event to topic test: key = 5            value = {"value": "v_5", "timestamp": 1652162956.099496}
Produced event to topic test: key = 7            value = {"value": "v_7", "timestamp": 1652162956.105106}
Produced event to topic test: key = 3            value = {"value": "v_3", "timestamp": 1652162956.107973}
Produced event to topic test: key = 8            value = {"value": "v_8", "timestamp": 1652162956.108137}
Produced event to topic test: key = 4            value = {"value": "v_4", "timestamp": 1652162956.107795}
Produced event to topic test: key = 2            value = {"value": "v_2", "timestamp": 1652162956.104955}
Produced event to topic test: key = 1            value = {"value": "v_1", "timestamp": 1652162956.100667}
Produced event to topic test: key = 9            value = {"value": "v_9", "timestamp": 1652162956.108047}
$ ./consume_driver.py basic.ini 2 
{'bootstrap.servers': '129.114.108.39:9092,129.114.109.13:9092,129.114.108.242:9092', 'topic': 'test', 'group.id': 'experiment_group_1', 'timeout': '1', 'auto.offset.reset': 'earliest'}
*** 7386 *** topic test (partitions: [0, 1]): key = 0            value = {"value": "v_0", "timestamp": 1652162956.099297} (6.465930938720703 sec used)
LAG @ partition 0: 199 - -1001 = 1200
LAG @ partition 1: 219 - 215 = 4
*** 7386 *** topic test (partitions: [0, 1]): key = 3            value = {"value": "v_3", "timestamp": 1652162956.107973} (6.998964786529541 sec used)
LAG @ partition 0: 199 - -1001 = 1200
LAG @ partition 1: 219 - 216 = 3
*** 7386 *** topic test (partitions: [0, 1]): key = 8            value = {"value": "v_8", "timestamp": 1652162956.108137} (7.587634086608887 sec used)
LAG @ partition 0: 199 - -1001 = 1200
LAG @ partition 1: 219 - 217 = 2
*** 7386 *** topic test (partitions: [0, 1]): key = 9            value = {"value": "v_9", "timestamp": 1652162956.108047} (8.157027959823608 sec used)
LAG @ partition 0: 199 - -1001 = 1200
LAG @ partition 1: 219 - 218 = 1
*** 7386 *** topic test (partitions: [0, 1]): key = 2            value = {"value": "v_2", "timestamp": 1652162956.104955} (8.772356986999512 sec used)
LAG @ partition 0: 199 - -1001 = 1200
LAG @ partition 1: 219 - 219 = 0
*** 7386 *** topic test (partitions: [0, 1]): key = 1            value = {"value": "v_1", "timestamp": 1652162956.100667} (9.362993955612183 sec used)
LAG @ partition 0: 199 - -1001 = 1200
LAG @ partition 1: 219 - 220 = -1
*** 7385 *** topic test (partitions: [0]): key = 6            value = {"value": "v_6", "timestamp": 1652162956.100889} (10.505061149597168 sec used)
LAG @ partition 0: 199 - 197 = 2
*** 7385 *** topic test (partitions: [0]): key = 5            value = {"value": "v_5", "timestamp": 1652162956.099496} (11.051942110061646 sec used)
LAG @ partition 0: 199 - 198 = 1
*** 7385 *** topic test (partitions: [0]): key = 7            value = {"value": "v_7", "timestamp": 1652162956.105106} (11.741386890411377 sec used)
LAG @ partition 0: 199 - 199 = 0
*** 7385 *** topic test (partitions: [0]): key = 4            value = {"value": "v_4", "timestamp": 1652162956.107795} (12.283226013183594 sec used)
LAG @ partition 0: 199 - 200 = -1
$
```