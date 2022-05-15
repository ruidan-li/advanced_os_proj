# when calling bmk.sh:
# rf=$1 # replication factor
# pa=$2 # partition count
# co=$3 # num. consumer
# po=$4 # num. producer
# vr=$5 # version of the code

rf=3
vr=3
partition_2_4() { # pa=$1
for co in 2 4 6 8; do
    for po in 2 4 6 8; do
        echo $rf $1 $co $po $vr
        . bmk.sh $rf $1 $co $po $vr
        sleep 1
    done
done
}

partition_8_16() { # pa=$1
    for co in 2 4 6 8; do
        for po in 2 4 6 8; do
            echo $rf $1 $co $po $vr
            . bmk.sh $rf $1 $co $po $vr
            sleep 1
        done
    done
}

partition_2_4 2
partition_2_4 4
partition_8_16 8
partition_8_16 16
