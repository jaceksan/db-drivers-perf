import argparse
import csv
import glob
import json
import logging
from logging import Logger
import os
import re
from pathlib import Path
from time import time
from typing import Any
from dotenv import load_dotenv
from tabulate import tabulate

import adbc_driver_postgresql.dbapi
import adbc_driver_snowflake.dbapi
from turbodbc import Megabytes, connect, make_options
from python_libs.config import Database, Location, read_config
from python_libs.result import JavaResult, PythonResult, PythonResults

PATH_TO_RESULTS = Path("results")
PATH_TO_RESULT_FILES = PATH_TO_RESULTS / "result_files"
ERROR_MSG = "Error"
UNSUPPORTED_MSG = "Unsupported"


class PoCDbDriversExecutor:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger("PoCDbDriversExecutor")
        self.logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(levelname)s - %(name)s - %(message)s")
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    @staticmethod
    def get_duration(start):
        return int((time() - start) * 1000)

    def info(self, use_case, msg, iteration=None):
        if iteration:
            self.logger.info(f"{use_case}({iteration=}) - {msg}")
        else:
            self.logger.info(f"{use_case} - {msg}")

    @staticmethod
    def execute_select(connection, query):
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor

    @staticmethod
    def execute_and_fetch_arrow_turbodbc(connection, query):
        row_count = 0
        with connection.cursor() as cursor:
            cursor.execute(query)
            batches = cursor.fetcharrowbatches()
            for batch in batches:
                row_count += batch.num_rows
                # result.extend(batch)
            return row_count

    @staticmethod
    def execute_and_fetch_arrow_adbc(connection, query):
        row_count = 0
        result = []
        with connection.cursor() as cursor:
            cursor.execute(query)
            batches = cursor.fetch_record_batch()
            for batch in batches:
                row_count += batch.num_rows
                result.extend(batch)
            return row_count

    def execute_and_fetch_standard(self, connection, query):
        with self.execute_select(connection, query) as cursor:
            return cursor.fetchall()

    def report_fetch_finished(self, use_case, iteration, duration, rows=None):
        if rows:
            self.info(
                use_case, f"Cursor fetch finished in {duration} ms {rows=}", iteration
            )
        else:
            self.info(use_case, f"Cursor fetch finished in {duration} ms", iteration)

    def report_file_write_finished(self, use_case, duration):
        self.info(use_case, f"Write to file finished in {duration} ms")

    def report_finished(self, use_case, duration):
        self.info(use_case, f"Use case finished in {duration} ms")

    @staticmethod
    def average(lst: list[float]) -> float:
        if len(lst) == 0:
            return 0
        return float(sum(lst) / len(lst))

    def execute_use_case(
        self,
        connection_type: str,
        limit: int,
        location: Location,
        database: Database,
        query: str,
        connect_func,
        connect_params: dict[str, Any],
        iterations: int,
        exec_fetch_func,
    ) -> PythonResult:
        use_case = self.make_use_case(connection_type, location.name, database.name)
        self.logger.debug(f"Running use case {use_case}")
        durations = []
        error = None
        try:
            with connect_func(**connect_params) as connection:
                for i in range(1, iterations + 1):
                    start = time()
                    self.info(use_case, "START", i)
                    result = exec_fetch_func(connection, query)
                    duration = self.get_duration(start)
                    self.report_fetch_finished(use_case, i, duration, result)
                    durations.append(duration)
        except Exception as e:
            self.logger.error(f"Error running use case {use_case}: {e}")
            error = str(e)
        finally:
            return PythonResult(
                limit=limit,
                location=location.name,
                database=database.name,
                connection_type=connection_type,
                durations=durations,
                avg_duration=self.average(durations),
                error=error,
            )

    @staticmethod
    def make_use_case(connection_type, location, db_name):
        return f"{connection_type}_{location}_{db_name}"

    @staticmethod
    def append_result(
        result, destination, db_name, use_case, fetch_duration, write_duration
    ):
        result.append([destination, db_name, use_case, fetch_duration, write_duration])

    @staticmethod
    def prepare_result_folders():
        if not os.path.exists(PATH_TO_RESULTS):
            os.makedirs(PATH_TO_RESULTS)
        if not os.path.exists(PATH_TO_RESULT_FILES):
            os.makedirs(PATH_TO_RESULT_FILES)

    @staticmethod
    def get_report_results_table_without_write(
        results: PythonResults, write_to_csv=False
    ):
        rows = []
        header = ["Limit", "Location", "Database", "Connection type", "Duration"]
        for result in results.results:
            if not write_to_csv and result.error:
                rows.append(
                    [
                        result.limit,
                        result.location,
                        result.database,
                        result.connection_type,
                        result.error,
                    ]
                )
            else:
                if write_to_csv:
                    # Write all executions to CSV
                    for duration in result.durations:
                        rows.append(
                            [
                                result.limit,
                                result.location,
                                result.database,
                                result.connection_type,
                                duration,
                            ]
                        )
                else:
                    # Report only average duration to STDOUT, limit decimal points to 2
                    rows.append(
                        [
                            result.limit,
                            result.location,
                            result.database,
                            result.connection_type,
                            f"{result.avg_duration:.2f}",
                        ]
                    )
        return header, rows

    def read_java_result(self, file, location, database) -> list[PythonResult]:
        results = []
        with open(file) as jsonfile:
            java_results = [JavaResult.from_dict(jr) for jr in json.load(jsonfile)]
            for java_result in java_results:
                durations = java_result.primaryMetric.rawData[0]
                avg_duration = self.average(durations)
                results.append(
                    PythonResult(
                        limit=int(java_result.params.limit),
                        location=location,
                        database=database,
                        connection_type=f"java_{java_result.params.connectionType.lower()}",
                        durations=durations,
                        avg_duration=avg_duration,
                        error=None,
                    )
                )
        return results

    def read_java_results(self, results: PythonResults) -> None:
        re_file = re.compile(r"java_results_([^_]+)_([^_]+).json")
        for file in glob.glob(str(PATH_TO_RESULTS / "java_results_*.json")):
            if (match := re_file.search(file)) is not None:
                location, database = match.groups()
                results.results.extend(self.read_java_result(file, location, database))

    def report_results(self, results: PythonResults):
        header, rows = self.get_report_results_table_without_write(results)
        print(tabulate(rows, headers=header, tablefmt="psql"))

    def write_results_csv(self, results: PythonResults, result_file):
        header, rows = self.get_report_results_table_without_write(
            results, write_to_csv=True
        )
        with open(PATH_TO_RESULTS / result_file, "w") as csvfile:
            csv_writer = csv.writer(
                csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
            )
            csv_writer.writerow(header)
            csv_writer.writerows(rows)


class PoCDbDrivers:
    def __init__(self):
        load_dotenv()
        self.args = self.parse_arguments()
        self.config = read_config()
        self.iterations = self.config.config.measurement_iterations
        self.limits = [1_000, 10_000, 100_000, 1_000_000, 5_000_000]
        self.executor = PoCDbDriversExecutor(self.args)
        self.logger = self.get_logger()

    @staticmethod
    def get_logger() -> Logger:
        logger = logging.getLogger("PoCDbDrivers")
        logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(levelname)s - %(name)s - %(message)s")
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    @staticmethod
    def parse_arguments() -> argparse.Namespace:
        # noinspection PyTypeChecker
        parser = argparse.ArgumentParser(
            conflict_handler="resolve",
            description="Compare DB connection types (ADBC, TurboODBC).",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "-r",
            "--result-file",
            default="all_results.csv",
            help="Result CSV file name",
        )
        return parser.parse_args()

    def run_turbodbc(
        self, limit: int, location: Location, database: Database, query: str
    ) -> PythonResult:
        driver = f"Driver={database.odbc_driver_path}"
        server = f"Server={database.host}"
        port = f"Port={database.port}"
        db_name = f"Database={database.db_name}"
        user = f"UID={database.user}"
        password = f"PWD={database.password}"
        static_props = ""
        turbodbc_options = make_options()
        if database.db_type == "POSTGRESQL":
            static_props = (";Protocol=7.4;UseDeclareFetch=1;Fetch=10000;" +
                            "UseServerSidePrepare=1;BoolsAsChar=0;USESSL=true;SSLmode=prefer")
            # These options work only for PostgreSQL, segmentation fault when using it with Vertica
            turbodbc_options = make_options(
                read_buffer_size=Megabytes(1),
                parameter_sets_to_buffer=1000,
                use_async_io=True,
                # note: this incurs about 20-30% performance overhead at least when talking with PostgreSQL
                # prefer_unicode=True,
                autocommit=False,
            )
        odbc_conn_str = (
            f"{driver};{server};{port};{db_name};{user};{password}{static_props}"
        )
        return self.executor.execute_use_case(
            connection_type="python_turbodbc",
            limit=limit,
            location=location,
            database=database,
            query=query,
            connect_func=connect,
            connect_params={
                "connection_string": odbc_conn_str,
                "turbodbc_options": turbodbc_options,
            },
            iterations=self.iterations,
            exec_fetch_func=self.executor.execute_and_fetch_arrow_turbodbc,
        )

    def run_python_adbc(
        self, limit: int, location: Location, database: Database, query: str
    ) -> PythonResult:
        connection_type = "python_adbc"
        if database.db_type == "VERTICA":
            return PythonResult(
                limit=limit,
                location=location.name,
                database=database.name,
                connection_type=connection_type,
                durations=[],
                avg_duration=-1,
                error=UNSUPPORTED_MSG,
            )

        conn_kwargs = {
            "host": database.host,
            "port": database.port,
            "database": database.db_name,
            "user": database.user,
            "password": database.password,
        }
        if database.db_type == "SNOWFLAKE":
            connect_func = adbc_driver_snowflake.dbapi.connect
            uri = f"{database.user}:{database.password}@{database.account}/{database.db_name}?warehouse={database.warehouse}"
            conn_kwargs["warehouse"] = database.warehouse
            conn_kwargs["account"] = database.account
        else:
            connect_func = adbc_driver_postgresql.dbapi.connect
            uri = f"postgresql://{database.user}:{database.password}@{database.host}:{database.port}/{database.db_name}"
        return self.executor.execute_use_case(
            connection_type=connection_type,
            limit=limit,
            location=location,
            database=database,
            query=query,
            connect_func=connect_func,
            connect_params={"uri": uri},
            iterations=self.iterations,
            exec_fetch_func=self.executor.execute_and_fetch_arrow_adbc,
        )

    def calculate_limits(self, database: Database) -> list[int]:
        bottom_limit = database.bottom_limit or 1_000
        top_limit = database.top_limit or 5_000_000
        return [x for x in self.limits if bottom_limit <= x <= top_limit]

    def main(self):
        start = time()
        query_file = self.config.config.query
        with open(query_file) as fp:
            query_raw = fp.read()
        results = PythonResults(results=[])
        self.executor.prepare_result_folders()
        for location in self.config.locations:
            self.logger.info(f"Running with location {location.name}")
            for database in location.databases:
                self.logger.info(f"Running with database {database.name}")
                limits = self.calculate_limits(database)
                for limit in limits:
                    self.logger.info(f"Running with limit {limit}")
                    query = query_raw + f" LIMIT {limit}"
                    results.results.append(
                        self.run_python_adbc(limit, location, database, query)
                    )
                    results.results.append(
                        self.run_turbodbc(limit, location, database, query)
                    )

        self.executor.read_java_results(results)
        self.executor.report_results(results)
        self.executor.write_results_csv(results, self.args.result_file)
        self.executor.report_finished(
            "All use cases", self.executor.get_duration(start)
        )


if __name__ == "__main__":
    PoCDbDrivers().main()
