# when calling bmk.sh:
# rf=$1 # replication factor
# pa=$2 # partition count
# co=$3 # num. consumer
# po=$4 # num. producer
# vr=$5 # version of the code
# op=$6 number of operations
# sa=$7 sampling interval (deprecated, use line 22, 27 of consumer.py instead)

rf=3
vr=1025
op=750000
sa=100

partition() {
    pa=$1
    co=$2
    po=$3
    echo       $rf $pa $co $po $vr $op $sa
    . ./bmk.sh $rf $pa $co $po $vr $op $sa
}

partition 2 2 2
