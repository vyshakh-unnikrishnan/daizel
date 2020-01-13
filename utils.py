# Databricks notebook source
try:
  env = dbutils.widgets.get("environment")
except:
  env = "dev"
  #TODO: logging

# COMMAND ----------

#Properties Azure SQL Database
if env == "dev":
    db_hostname = 'xto-mbc-eu-zlsd-sqlsrv-zlsdsrv.database.windows.net'
    db_port = 1433
    db_database = 'xtombceuzlsdsqldb'
    db_url = 'jdbc:sqlserver://{0}:{1};database={2}'.format(db_hostname, db_port, db_database)
    db_properties = {}
    db_properties['username'] = 'ConZertDatabricks'
    db_properties['password'] = dbutils.secrets.get(scope = "kv-secrets-conzert", key = "ConZertDatabricks")

# COMMAND ----------

#Properties ADLS
if env == "dev":
  path_azst = "/mnt/zlsdev/test_source/azst"
  path_cis2 = "/mnt/zlsdev/test_source/cis2"
  path_synt = "/mnt/zlsdev/test_source/synthetic"
if env == "int":
  path_azst = "/mnt/zlsdev/test_source/azst"
  path_cis2 = "/mnt/xtordeudladls/CIS2/INT"
  path_synt = "/mnt/zlsdev/test_source/synthetic"
elif env == "prod":
  path_azst = "/mnt/zlsdev/test_source/azst"
  path_cis2 = "/mnt/xtordeudladls/CIS2/PROD"
  path_synt = "/mnt/zlsdev/test_source/synthetic"

# COMMAND ----------

#generate load_date_int without dashes
year = load_date[0:4]
month = load_date[5:7]
day = load_date[8:10]
load_date_int = year + month + day

# COMMAND ----------

def print_env():
  print(env)

# COMMAND ----------

def read_csv_from_adls(path,table):
  return spark.read.load(path,format="csv",sep=";",lineSep="\n",inferSchema="true",header="true",multiLine="true",ignoreTrailingWhiteSpace="true")

# COMMAND ----------

def read_table_from_sqldb(table_name):
  return spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)

# COMMAND ----------

def read_table_from_sqldb_wth_filter(table_name, filter_str, distinct=False):
  df = spark.read.jdbc(url=db_url
                       , table=table_name
                       , properties=db_properties
                        ).where(filter_str)
  if distinct:
    df = df.distinct()
  return df

# COMMAND ----------

def read_table_from_sqldb_wth_distinct(table_name):
  df = spark.read.jdbc(url=db_url
                       , table=table_name
                       , properties=db_properties
                        ).distinct()
  return df

# COMMAND ----------

def drop_technical_columns(df, columns_to_drop = ['id', 'insert_ts']):
  return df.drop(*columns_to_drop)

# COMMAND ----------

def write_table_to_sqldb(df, table_name, mode="error"):
  df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=db_properties)

# COMMAND ----------

def read_parquet_from_adls(file_path):
  return spark.read.parquet(file_path)