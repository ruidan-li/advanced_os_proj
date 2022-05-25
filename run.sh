# $1 num. consumer
# $2 num. producer
# $3 config file
# $4 num of operations
# $5 sampling interval

source env/bin/activate
backup_dir=/tmp/vr-$(expr $6 - 1)/
mkdir $backup_dir
mv logs $backup_dir 
mv logs_cntr $backup_dir 
mv logs_time $backup_dir 
mkdir -p logs logs_cntr logs_time
cd src
./consume_driver.py $3 $1 $5 2 120 350 > tmp.out &
sleep 2
./produce_driver.py $3 $2 $4 -s
cd ..
wait
