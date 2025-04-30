# Add the project root directory to sys.path
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import Row, SparkSession
from src.utils.pyspark_utils import calculate_average_by_shop
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Initialize Spark session for testing
spark = SparkSession.builder.appName("test_pyspark_utils").getOrCreate()

# Sample test data
test_data = [
    Row(id=1, name="Apple", price=1.35, shop_name="Shop_1"),
    Row(id=2, name="Banana", price=2.35, shop_name="Shop_2"),
    Row(id=3, name="Carrot", price=1.35, shop_name="Shop_1"),
    Row(id=4, name="Onion", price=3.35, shop_name="Shop_2"),
]

test_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("shop_name", StringType(), True)
])

test_df = spark.createDataFrame(test_data, schema=test_schema)

# Expected result
expected_data = [
    Row(shop_name="Shop_1", average_price=1.35),
    Row(shop_name="Shop_2", average_price=2.85),
]

expected_df = spark.createDataFrame(expected_data, schema=StructType([
    StructField("shop_name", StringType(), True),
    StructField("average_price", DoubleType(), True)
]))

# Run the function
result_df = calculate_average_by_shop(test_df)

# Assert the results using approximate equality for floating-point values
def assert_dataframes_equal(df1, df2):
    assert df1.schema == df2.schema, "Schemas do not match"
    df1_data = sorted(df1.collect(), key=lambda row: row.shop_name)
    df2_data = sorted(df2.collect(), key=lambda row: row.shop_name)
    for row1, row2 in zip(df1_data, df2_data):
        assert row1.shop_name == row2.shop_name, f"Shop names do not match: {row1.shop_name} != {row2.shop_name}"
        assert abs(row1.average_price - row2.average_price) < 1e-6, f"Average prices do not match: {row1.average_price} != {row2.average_price}"

assert_dataframes_equal(result_df, expected_df)
print("Unit test passed!")
