# $1 num. consumer
# $2 num. producer
# $3 config file
# $4 num of operations
# $5 sampling interval

source env/bin/activate
rm -rf logs logs_cntr logs_time
mkdir -p logs logs_cntr logs_time
cd src
./consume_driver.py $3 $1 $5 1 60 350 > tmp.out &
sleep 2
./produce_driver.py $3 $2 $4 -s
cd ..
wait
