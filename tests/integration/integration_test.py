# Databricks notebook source
from my_package.code import columns_except
import dlt

# COMMAND ----------

# get data path from the DLT Pipeline configuration so we can test the pipeline with smaller amount of data
default_json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/"
json_path = spark.conf.get("my_etl.data_path", default_json_path)
print(f"Loading data from {json_path}")

# COMMAND ----------

@dlt.table(
   comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
  return spark.read.format("json").load(json_path)

# COMMAND ----------

@dlt.table(
  comment="Leave only links, and exclude some columns from dataset"
)
@dlt.expect_or_drop("only links", "type is not null and type in ('link', 'redlink')")
def clickstream_filtered():
  df = dlt.read("clickstream_raw")
  # use imported function
  new_cols = columns_except(df, ['prev_id', 'prev_title'])
  return df.select(*new_cols)

# COMMAND ----------

@dlt.table(comment="Check number of records")
@dlt.expect_or_fail("valid count", "count = 12480649 or count = 0") # we need to check for 0 because DLT first evaluates with empty dataframe
def filtered_count_check():
  cnt = dlt.read("clickstream_filtered").count()
  return spark.createDataFrame([[cnt]], schema="count long")

# COMMAND ----------

@dlt.table(comment="Check type")
@dlt.expect_all_or_fail({"valid type": "type in ('link', 'redlink')",
                         "type is not null": "type is not null"})
def filtered_type_check():
  return dlt.read("clickstream_filtered").select("type")
