rf=$1 # replication factor
pa=$2 # partition count
co=$3 # num. consumer
po=$4 # num. producer
vr=$5 # version of the code
op=$6 # number of operations
sa=$7 # sampling interval
cs=$8 # num. consumer to put to sleep

cd src

# generate a topic
python3 admin.py basic.ini $rf $pa $co $po $vr
topic="topic-rf$rf-pa$pa-co$co-po$po-vr$vr"
echo $topic

# update ini file (update the topic)
sed "s/topic=test/topic=$topic/g; s/partition.num=xx/partition.num=$pa/g" basic.ini > current.ini
cd ..

# adjust run.sh
. ./run.sh $co $po current.ini $op $sa $vr $cs

# run analysis.py
mv ./res/$topic /tmp/$topic
mkdir -p ./res/$topic
python3 analysis3.py ./res/$topic
