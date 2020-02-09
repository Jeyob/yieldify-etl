# yieldify-etl

This is a small etl service application written in python (3.6+). It is a lightweight CLI tool using sqlite3 as database and the flask webframework for exposing an web api


## Requirements
- python 3.7 or above
- pip3
## Install instructions
1. Install pipenv using pip
    ````
    pip install pipenv 
    ````
2. Change directory into the application folder 'yieldify-etl' where you should see two files named Pipfile and Pipfile.lock
3. Create virtual environment and install required packages within it
    ````
    pipenv install Pipfile.lock
    ````
## How to run
For help on available commands use -h flag where relevant: e.g.
    ````
    pipenv run main.py -h
    ````

1. Create database and extract input file (input_data.gz)*
    ````
    pipenv run main.py -c main.conf rebuild-database <gzipped input data>
    ````
2. Print stats to stdout
    ````
    pipenv run main.py -c main.conf run -t stdout
    ````
3. Create database and extract input data and print to stdout
    ```bash
    pipenv run main.py -c main.conf rebuild-database <gzipped input data> run -t stdout
    ```
4. Provide stats through api
    ````
    pipenv run main.py -c main.conf run -t api
    ````
   This will trigger to run flask's dev webserver on ```localhost:8080```. Open a browser or any other http tool for making a GET request.
   
   For example:
   ```
   localhost:8080/stats/browser?start_date=2014-10-11%2000:00:00&end_date=2014-10-13%2018:01:01
   ```
   Result:
   ```json
    [
     {"browser":"Mobile Safari","percentage":0.2980790074},
     {"browser":"Chrome","percentage":0.2649029181},
     {"browser":"IE","percentage":0.1788866294},
     {"browser":"Firefox","percentage":0.082486039},
     {"browser":"Safari","percentage":0.0690256717}
   ]
   ```