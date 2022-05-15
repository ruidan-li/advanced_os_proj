# advanced_os_proj
Course project for advanced OS. Kafka pipeline

https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html

### Setup
```
$ source setup.sh 
```

### Example run
* To produce: `./produce_driver.py config_file num_proc msg_count`
* To consume: `./consume_driver.py config_file num_proc`
    * config_file - path to the configuration file
    * num_proc - how many processes to spin up
    * msg_count - total number of messages to produce

See example:
```
$ cd src 

$ ./produce_driver.py basic.ini 5 10
{'bootstrap.servers': '129.114.108.39:9092,129.114.109.13:9092,129.114.108.242:9092', 'topic': 'test'}
Produced event to topic test: key = 0            value = {"value": "v_0", "timestamp": 1652163604.641698}
Produced event to topic test: key = 2            value = {"value": "v_2", "timestamp": 1652163604.641683}
Produced event to topic test: key = 3            value = {"value": "v_3", "timestamp": 1652163604.641683}
Produced event to topic test: key = 8            value = {"value": "v_8", "timestamp": 1652163604.642021}
Produced event to topic test: key = 1            value = {"value": "v_1", "timestamp": 1652163604.641696}
Produced event to topic test: key = 9            value = {"value": "v_9", "timestamp": 1652163604.642039}
Produced event to topic test: key = 5            value = {"value": "v_5", "timestamp": 1652163604.642017}
Produced event to topic test: key = 6            value = {"value": "v_6", "timestamp": 1652163604.64203}
Produced event to topic test: key = 4            value = {"value": "v_4", "timestamp": 1652163604.64168}
Produced event to topic test: key = 7            value = {"value": "v_7", "timestamp": 1652163604.642073}

$ ./consume_driver.py basic.ini 2
{'bootstrap.servers': '129.114.108.39:9092,129.114.109.13:9092,129.114.108.242:9092', 'topic': 'test', 'group.id': 'experiment_group_1', 'timeout': '1', 'auto.offset.reset': 'earliest'}
*** 8730 *** topic test (partitions: [1]): key = 0            value = {"value": "v_0", "timestamp": 1652163604.641698} (13.505370140075684 sec used)
*** 8729 *** topic test (partitions: [0]): key = 5            value = {"value": "v_5", "timestamp": 1652163604.642017} (13.505030155181885 sec used)
LAG @ partition 1: 225 - 221 = 4
LAG @ partition 0: 203 - 201 = 2
*** 8730 *** topic test (partitions: [1]): key = 3            value = {"value": "v_3", "timestamp": 1652163604.641683} (14.060750007629395 sec used)
*** 8729 *** topic test (partitions: [0]): key = 6            value = {"value": "v_6", "timestamp": 1652163604.64203} (14.060420036315918 sec used)
LAG @ partition 1: 225 - 222 = 3
*** 8730 *** topic test (partitions: [1]): key = 8            value = {"value": "v_8", "timestamp": 1652163604.642021} (14.688874959945679 sec used)
LAG @ partition 0: 203 - 202 = 1
*** 8729 *** topic test (partitions: [0]): key = 4            value = {"value": "v_4", "timestamp": 1652163604.64168} (14.691976070404053 sec used)
LAG @ partition 1: 225 - 223 = 2
LAG @ partition 0: 203 - 203 = 0
*** 8730 *** topic test (partitions: [1]): key = 2            value = {"value": "v_2", "timestamp": 1652163604.641683} (15.393854856491089 sec used)
*** 8729 *** topic test (partitions: [0]): key = 7            value = {"value": "v_7", "timestamp": 1652163604.642073} (15.394057035446167 sec used)
LAG @ partition 1: 225 - 224 = 1
LAG @ partition 0: 203 - 204 = -1
*** 8730 *** topic test (partitions: [1]): key = 9            value = {"value": "v_9", "timestamp": 1652163604.642039} (16.009943962097168 sec used)
LAG @ partition 1: 225 - 225 = 0
*** 8730 *** topic test (partitions: [1]): key = 1            value = {"value": "v_1", "timestamp": 1652163604.641696} (16.55230402946472 sec used)
LAG @ partition 1: 225 - 226 = -1

$
```

### Update 05/11/22
* Added silent mode in producer
* Added sample interval arg in consumer (default to 5,000)
See example below:
```
$ ./produce_driver.py basic.ini 5 5000 -s
{'bootstrap.servers': '129.114.108.39:9092,129.114.109.13:9092,129.114.108.242:9092', 'topic': 'test'}

$ ./consume_driver.py basic.ini 2 1000 > tmp.out

$ grep Writing 'tmp.out'
Writing metric to /tmp/kafka_run_569ed640-3140-4231-9851-47b5bd92f0ca.out
Writing metric to /tmp/kafka_run_24605fdc-55f0-4b8a-9120-84f8b384fc33.out

$ cat /tmp/kafka_run_569ed640-3140-4231-9851-47b5bd92f0ca.out
timestamp,partition,latest_offset,current_position
1652281034.934373,1,5261,3760
1652281035.552668,1,5261,4760% 

$ cat /tmp/kafka_run_24605fdc-55f0-4b8a-9120-84f8b384fc33.out
timestamp,partition,latest_offset,current_position
1652281034.934477,0,5207,3710
1652281035.552669,0,5207,4710%
$
```

### Update 05/15/22
in the advance_os_proj folder, run
```
source env/bin/activate   
. bmk_multi.sh            # run multiple experiments (adjust parameters inside)
```