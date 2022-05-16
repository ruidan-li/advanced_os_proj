rf=$1 # replication factor
pa=$2 # partition count
co=$3 # num. consumer
po=$4 # num. producer
vr=$5 # version of the code
op=$6 # number of operations
sa=$7 # sampling interval

cd src

# generate a topic
python3 admin.py basic.ini $rf $pa $co $po $vr
topic="topic-rf$rf-pa$pa-co$co-po$po-vr$vr"
echo $topic

# update ini file (update the topic)
sed "s/topic=test/topic=$topic/g" basic.ini > current.ini
cd ..

# adjust run.sh
. run.sh $co $po current.ini $op $sa

# run analysis.py
rm -rf ./res/$topic
mkdir -p ./res/$topic
python3 analysis.py ./res/$topic