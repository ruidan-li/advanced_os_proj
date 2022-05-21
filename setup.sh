if [[ ! -d ./env ]]
then
    echo "Create virtual env"
    python3 -m venv env
else
    echo "Virtual env exists"
fi

source env/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

chmod +x ./src/*_driver.py
