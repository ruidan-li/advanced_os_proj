# when calling bmk.sh:
# rf=$1 # replication factor
# pa=$2 # partition count
# co=$3 # num. consumer
# po=$4 # num. producer
# vr=$5 # version of the code
# op=$6 number of operations
# sa=$7 sampling interval (deprecated, use line 22, 27 of consumer.py instead)

rf=3
vr=226
op=10000000
sa=100
ps=0.00006               # producer sleep time, no need to pass as an argument.
echo $ps > sleeptime.txt # when a producer runs, it checks this file every 5 secs

partition() {
    pa=$1
    co=$2
    po=$3
    echo       $rf $pa $co $po $vr $op $sa $ps
    . ./bmk.sh $rf $pa $co $po $vr $op $sa
}

partition 4 4 4
