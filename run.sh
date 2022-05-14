source env/bin/activate
rm -rf logs
mkdir -p logs
cd src
./consume_driver.py basic.ini 2 100 &
./produce_driver.py basic.ini 2 100000
cd ..
wait