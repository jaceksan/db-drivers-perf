# PoC comparing turbodbc with other DB drivers

## Prepare env

```shell
# Download DB drivers. JDBC are no longer necessary(pom.xml), but ODBC drivers are.
./download_drivers.sh
# Build a simple Java app executing SQL query, fetching results, and converting them to Arrow format
mvn package
# Install additional libs to your OS (if necessary)
# Update python version based on what default python version you have
# sudo apt install libboost-all-dev unixodbc-dev python-dev libpython3.10-dev
sudo apt install libboost-all-dev unixodbc-dev libpython3.11-dev
# Setup Python env. Update python version based on what default python version you have
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Install Snowflake ODBC driver. Follow instructions here: https://docs.snowflake.com/en/user-guide/odbc.html
```

## Prepare local databases
In GoodData, go to `gdc-nas` directory. Run the following commands to start local databases.
```shell
docker-compose up -d postgres
docker-compose -f docker-compose-datasources.yaml up -d vertica
```
If you are not working in GooData, start databases your way.

## Remote databases
Currently drivers support fully PostgreSQL and Snowflake. Turbodbc supports Vertica as well.
You can start these databases in the cloud and test them.

## Credentials
See [env_example](env_example) for the list of environment variables that need to be set.

## Configure custom databases
Update `config.yaml` based on where your databases are running.

## Load large TPC-H dataset
In GoodData we have a tooling for loading various testing models including TPCH 1G.

```shell
./data_load.py -w tpch_1D0g -d pg_local --skip-download -nv
DB_USER=tiger ./data_load.py -w tpch_1D0g -d vertica_local --skip-download -nv
./data_load.py -w tpch_1D0g -d snowflake_eu_central --skip-download -nv
./data_load.py -w tpch_1D0g -d pg_eu_central --skip-download -nv
```

## Run Java test
It tests pure JDBC, JDBC + conversion to Arrow and Java ADBC driver.
It stores results in `results/jaba_results_*.json` files.
Python tooling then reads Java results and combine them with Python results.

Run Java app:
```shell
./run_java.sh
```

## Run Python test

Examples:
```shell
python poc_drivers.py --help
# You can specify a custom result.csv file
python poc_drivers.py -r test.csv
```

Load results to MotherDuck:
```sql
use tiger_tests;
use perf_tests;

drop table if exists quiver_drivers;
create table quiver_drivers as
select * from read_csv_auto('/home/jacek/work/src/gooddata-quiver/PoC/drivers/results/all_results.csv');
-- Tests it works
select * from quiver_drivers limit 10;
```

This DB + schema are connected to a data source in Labs environment:
https://demo-se.labs.cloud.gooddata.com/

## Known issues
1. ADBC fails when connecting to "far" databases(could not begin COPY: SSL SYSCALL error: EOF detected). It is not able to handle the latency.
2. Sometimes Turbodbc segfaults.
