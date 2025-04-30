from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import pyspark.sql.functions as F

# Define schema for the products file
products_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("shop_name", StringType(), True)
])

# Function to read the products file
def read_products_file(spark, file_path):
    return spark.read.csv(file_path, schema=products_schema, header=True)

# Function to calculate the average value of products by each shop
def calculate_average_by_shop(df):
    return df.groupBy("shop_name").agg(F.avg("price").alias("average_price"))
