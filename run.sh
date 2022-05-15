source env/bin/activate
rm -rf logs *.txt
mkdir -p logs
cd src
./consume_driver.py $3 $1 100 &
./produce_driver.py $3 $2 100000
cd ..
wait