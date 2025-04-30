from pyspark.sql import SparkSession
from src.utils.pyspark_utils import read_products_file, calculate_average_by_shop

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ai-pyspark-pipeline").getOrCreate()

    # Read the products file
    products_file_path = "src/data/products.csv"
    products_df = read_products_file(spark, products_file_path)

    # Calculate the average price by shop
    average_price_by_shop = calculate_average_by_shop(products_df)

    # Display the results
    average_price_by_shop.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
