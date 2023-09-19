from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from configparser import ConfigParser

# Step 1: Load database details from properties file
config = ConfigParser()
config.read('properties.ini')
dbProperties = {
    "url": config.get('DEFAULT', 'url'),
    "user": config.get('DEFAULT', 'user'),
    "password": config.get('DEFAULT', 'password'),
    "driver": config.get('DEFAULT', 'driver')
}

# Step 3: Create a separate method for SparkSession creation
def create_spark_session():
    return SparkSession.builder.appName("Incremental Data Load").getOrCreate()

# Step 7: Functions to read data from PostgreSQL tables
def read_from_table(spark, table_name, properties):
    url = properties['url']
    df = spark.read.jdbc(url, table=table_name, properties=properties)
    return df

# Main method (starting point of the program)
if __name__ == '__main__':
    # Step 2: Create a SparkSession
    spark = create_spark_session()

    # Step 8: Read parquet files from local path
    customers_path = config.get('parquet', 'customers')
    items_path = config.get('parquet', 'items')
    order_details_path = config.get('parquet', 'order_details')
    orders_path = config.get('parquet', 'orders')
    salesperson_path = config.get('parquet', 'salesperson')
    ship_to_path = config.get('parquet', 'ship_to')

    # Read and display Customers table
    customers_df = spark.read.jdbc(dbProperties["url"], 'customers', properties=dbProperties)
    print("-- Customers DataFrame --")
    customers_df.show()

    # Read and display Items table
    items_df = spark.read.jdbc(dbProperties["url"], 'items', properties=dbProperties)
    print("-- Items DataFrame --")
    items_df.show()

    # Read and display Order Details table
    order_details_df = spark.read.jdbc(dbProperties["url"], 'order_details', properties=dbProperties)
    print("-- Order Details DataFrame --")
    order_details_df.show()

    # Read and display Orders table
    orders_df = spark.read.jdbc(dbProperties["url"], 'orders', properties=dbProperties)
    print("-- Orders DataFrame --")
    orders_df.show()

    # Read and display Salespersons table
    salesperson_df = spark.read.jdbc(dbProperties["url"], 'salesperson', properties=dbProperties)
    print("-- Salesperson DataFrame --")
    salesperson_df.show()

    # Read and display Ship To table
    ship_to_df = spark.read.jdbc(dbProperties["url"], 'ship_to', properties=dbProperties)
    print("-- Ship To DataFrame --")
    ship_to_df.show()


    # Step 6: Get the latest date from the DataFrames
    latest_date = customers_df.agg({"CREATED_DATE": "max"}).collect()[0][0]

    # Step 7: Create DataFrames from DB tables
    salesperson_df = read_from_table(spark, 'SALESPERSON', dbProperties)
    customers_df_db = read_from_table(spark, 'CUSTOMERS', dbProperties)
    orders_df_db = read_from_table(spark, 'ORDERS', dbProperties)
    items_df_db = read_from_table(spark, 'ITEMS', dbProperties)
    order_details_df_db = read_from_table(spark, 'ORDER_DETAILS', dbProperties)
    ship_to_df_db = read_from_table(spark, 'SHIP_TO', dbProperties)

    # Step 8: Filter records greater than latest date
    salesperson_df = salesperson_df.filter(salesperson_df["CREATED_DATE"] > latest_date)
    customers_df_db = customers_df_db.filter(customers_df_db["CREATED_DATE"] > latest_date)
    orders_df_db = orders_df_db.filter(orders_df_db["CREATED_DATE"] > latest_date)
    items_df_db = items_df_db.filter(items_df_db["CREATED_DATE"] > latest_date)
    order_details_df_db = order_details_df_db.filter(order_details_df_db["CREATED_DATE"] > latest_date)
    ship_to_df_db = ship_to_df_db.filter(ship_to_df_db["CREATED_DATE"] > latest_date)

    # Step 9: Write the DataFrames in append mode
    customers_df_db.write.mode('append').parquet(customers_path)
    items_df_db.write.mode('append').parquet(items_path)
    order_details_df_db.write.mode('append').parquet(order_details_path)
    orders_df_db.write.mode('append').parquet(orders_path)
    salesperson_df.write.mode('append').parquet(salesperson_path)
    ship_to_df_db.write.mode('append').parquet(ship_to_path)

    # Step 10: Apply latest date change in reportgenerator.py
    with open("reportgenerator.py", "r") as file:
        data = file.readlines()
    with open("reportgenerator.py", "w") as file:
        for line in data:
            file.write(line.replace("<latest_date_variable>", str(latest_date)))
