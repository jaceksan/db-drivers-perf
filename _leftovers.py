#####################################
# Leftovers, other connection types
#####################################

# import jaydebeapi
# import psycopg2
# import vertica_python
# import pyarrow.parquet as pq

# def run_jaydebeapi(self, use_case, destination, db_name, query):
#     with jaydebeapi.connect(
#         jclassname=self.config[destination][db_name]["jdbc_driver_class"],
#         url=self.config[destination][db_name]["jdbc_url"],
#         driver_args=[
#             self.config[destination][db_name]["user"],
#             self.config[destination][db_name]["password"],
#         ],
#         jars=self.config[destination][db_name]["jdbc_driver_path"],
#     ) as connection:
#         return self.executor.execute_use_case(
#             use_case=use_case,
#             query=query,
#             connection=connection,
#             exec_fetch_func=self.executor.execute_and_fetch_standard,
#             write_func=self.executor.write_data_to_csv,
#         )
#
# def vertica_python_csv(self, use_case, destination, db_name, query):
#     vp_conn_info = {
#         "host": self.config[destination][db_name]["host"],
#         "port": self.config[destination][db_name]["port"],
#         "user": self.config[destination][db_name]["user"],
#         "password": self.config[destination][db_name]["password"],
#         "database": self.config[destination][db_name]["database"],
#     }
#     with vertica_python.connect(**vp_conn_info) as connection:
#         return self.executor.execute_use_case(
#             use_case=use_case,
#             query=query,
#             connection=connection,
#             exec_fetch_func=self.executor.execute_and_fetch_standard,
#             write_func=self.executor.write_data_to_csv,
#         )
#
# def postgres_python_csv(self, use_case, destination, db_name, query):
#     with psycopg2.connect(
#         user=self.config[destination][db_name]["user"],
#         password=self.config[destination][db_name]["password"],
#         host=self.config[destination][db_name]["host"],
#         port=self.config[destination][db_name]["port"],
#         database=self.config[destination][db_name]["database"],
#     ) as connection:
#         return self.executor.execute_use_case(
#             use_case=use_case,
#             query=query,
#             connection=connection,
#             exec_fetch_func=self.executor.execute_and_fetch_standard,
#             write_func=self.executor.write_data_to_csv,
#         )
#
# def run_python_driver(self, use_case, destination, db_name, query):
#     if "vertica" in use_case:
#         return self.vertica_python_csv(use_case, destination, db_name, query)
#     else:
#         return self.postgres_python_csv(use_case, destination, db_name, query)

# @staticmethod
# def write_data_to_csv(use_case, data):
#     with open(PATH_TO_RESULT_FILES / f"result_{use_case}.csv", "w") as csvfile:
#         csv_writer = csv.writer(
#             csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
#         )
#         csv_writer.writerows(data)
#
# @staticmethod
# def write_table_to_parquet(db_name, table):
#     pq.write_table(table, where=PATH_TO_RESULT_FILES / f"result_{db_name}.parquet")

# Writing to CSV/PARQUET is not necessary for the PoC, we need to compare just how drivers perform when fetching from DB
# start = time()
# write_func(use_case, data)
# duration_write = self.get_duration(start)
# self.report_file_write_finished(use_case, duration_write)
