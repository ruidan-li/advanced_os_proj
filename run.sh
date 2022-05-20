# $1 num. consumer
# $2 num. producer
# $3 config file
# $4 num of operations
# $5 sampling interval

source env/bin/activate
rm -rf logs/*.out
mkdir -p logs
cd src
./consume_driver.py $3 $1 $5 > tmp.out &
./produce_driver.py $3 $2 $4 -s
cd ..
wait
