# Databricks notebook source
from runtime.nutterfixture import NutterFixture
from chispa.dataframe_comparer import assert_df_equality

from my_package import (
   add_missing_columns,
   columns_except,
   dataframe_except_columns,
   spark
)


class UnitTest(NutterFixture):
  def __init__(self):
    super().__init__()
    
  def assertion_columns_except(self):
    original_df = spark.createDataFrame([[1, 2, 3, 4]], schema="col1 int, col2 int, col3 int, col4 int")
    new_cols = columns_except(original_df, ["col2", "col4"])
    assert new_cols == ["col1", "col3"]
    
  def assertion_dataframe_except_columns(self):
    original_df = spark.createDataFrame(
        [[1, 2, 3, 4]], schema="col1 int, col2 int, col3 int, col4 int")
    new_df = dataframe_except_columns(original_df, ["col2", "col4"])
    expected_df = spark.createDataFrame([[1, 3]], schema="col1 int, col3 int")
    assert_df_equality(new_df, expected_df, ignore_nullable=True)
  
  def assertion_add_missing_columns(self):
    df1 = spark.createDataFrame([[1, 2]], schema="col1 int, col2 int")
    df2 = spark.createDataFrame([[1, "2", 3.0]], schema="col1 int, col4 string, col5 double")
    new_df = add_missing_columns(df1, df2)
    expected_df = spark.createDataFrame([[1, 2, None, None]], schema="col1 int, col2 int, col4 string, col5 double")
    assert_df_equality(new_df, expected_df, ignore_nullable=True)


if __name__ == "__main__":
    # Run the tests
    result = UnitTest().execute_tests()
    print(result.to_string())

    # Raise an exception if any of the tests failed
    if result.test_results.num_failures > 0:
        raise Exception("Test failed.")
