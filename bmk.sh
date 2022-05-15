# rf=1 # replication factor
# pa=8 # partition count
# co=2 # num. consumer
# po=4 # num. producer
# vr=2 # version of the code
rf=$1 # replication factor
pa=$2 # partition count
co=$3 # num. consumer
po=$4 # num. producer
vr=$5 # version of the code

cd src

# generate a topic
python3 admin.py basic.ini $rf $pa $co $po $vr
topic="topic-rf$rf-pa$pa-co$co-po$po-vr$vr"
echo $topic

# update ini file (update the topic)
sed "s/topic=test/topic=$topic/g" basic.ini > current.ini
cd ..

# adjust run.sh
. run.sh $co $po current.ini

# run analysis.py
rm -rf ./res/$topic
mkdir -p ./res/$topic
python3 analysis.py ./res/$topic