from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import configparser

def create_spark_session():
    """
    Create a SparkSession.
    """
    spark = SparkSession.builder \
        .appName("FullDataLoad") \
        .getOrCreate()
    return spark

def load_properties():
    """
    Load properties from properties file.
    """
    config = configparser.ConfigParser()
    config.read('properties.ini')
    return config['DEFAULT'], config['parquet']

# 1. Create a Spark session
spark = create_spark_session()

# 2. Load properties
default_properties, parquet_properties = load_properties()

# 3. Access configuration settings
customers_path = parquet_properties.get('customers')
items_path = parquet_properties.get('items')
order_details_path = parquet_properties.get('order_details')
orders_path = parquet_properties.get('orders')
salespersons_path = parquet_properties.get('salespersons')
ship_to_path = parquet_properties.get('ship_to')

# 4. Read parquet files
temp_cust = spark.read.parquet(customers_path)
temp_cust.createOrReplaceTempView("customers_view")

temp_item = spark.read.parquet(items_path)
temp_item.createOrReplaceTempView("items_view")

temp_details = spark.read.parquet(order_details_path)
temp_details.createOrReplaceTempView("order_details_view")

temp_orders = spark.read.parquet(orders_path)
temp_orders.createOrReplaceTempView("orders_view")

temp_sales = spark.read.parquet(salespersons_path)
temp_sales.createOrReplaceTempView("salespersons_view")

temp_ship = spark.read.parquet(ship_to_path)
temp_ship.createOrReplaceTempView("ship_to_view")

# Define queries
def execute_query(spark, query, table_name, properties):
    data = spark.sql(query).withColumn('current_date', current_date())
    print(f'--{table_name}--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table=table_name, properties=properties)
    data.coalesce(1).write.mode('overwrite').partitionBy('current_date').parquet(
        f'file:///C:/userstory2-output/query/{table_name}')

# Query Definitions
queries = [
    {
        'query': '''
            select c.cust_id,c.cust_name, count(od.ORDER_ID) as count
            from customers_view c  
            left join orders_view o on (c.cust_id= o.cust_id )
            left join order_details_view od on ( o.order_id = od.order_id)
            left join items_view i on (od.item_id = i.item_id)
            group by c.cust_id,c.cust_name
        ''',
        'table_name': 'query1_customer_wise_order_count'
    },
    {
        'query': '''
            select c.cust_id, c.cust_name, sum(od.DETAIL_UNIT_PRICE*od.ITEM_QUANTITY) as total_order
            from customers_view c left join orders_view o 
            on c.cust_id=o.cust_id left join order_details_view od
            on od.order_id=o.order_id left join items_view i
            on i.item_id=od.item_id
            group by c.cust_id, c.cust_name
        ''',
        'table_name': 'query2_customer_wise_sum_of_orders'
    },
    {
        'query': '''
            select i.item_id, i.item_description, sum(od.ITEM_QUANTITY) as total_order_count
            from items_view i left join order_details_view od on i.item_id=od.item_id
            group by i.item_id, i.item_description
            order by total_order_count desc
        ''',
        'table_name': 'query3_item_wise_Total_Order_count'
    },
    {
        'query': '''
            select i.CATEGORY, sum(od.ITEM_QUANTITY) as total_order_count
            from items_view i left join order_details_view od on i.item_id=od.item_id
            group by i.CATEGORY
            order by total_order_count desc
        ''',
        'table_name': 'query4_category_wise_Total_Order_count_descending'
    },
    {
        'query': '''
            select i.item_id, i.item_description, sum(i.unit_price*od.ITEM_QUANTITY) as total_order_amount
            from items_view i left join order_details_view od on i.item_id=od.item_id
            group by i.item_id, i.item_description
            order by total_order_amount desc
        ''',
        'table_name': 'query5_Item_wise_total_order_amount_in_descending'
    },
    {
        'query': '''
            select i.CATEGORY, sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as total_order_amount
            from items_view i left join order_details_view od on (od.item_id = i.item_id)
            group by i.CATEGORY
            order by total_order_amount desc
        ''',
        'table_name': 'query6_Item_name_category_wise_total_order_amount_in_descending'
    },
    {
        'query': '''
            select s.SALESMAN_ID, 
            sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) as total_order_amount, 
            (sum(od.ITEM_QUANTITY * od.DETAIL_UNIT_PRICE) * 0.1) as commission
            from salespersons_view s 
            left join customers_view c on (s.SALESMAN_ID = c.SALESMAN_ID)
            left join orders_view o on (c.cust_id= o.cust_id )
            left join order_details_view od on ( o.order_id = od.order_id)
            group by s.SALESMAN_ID

        ''',
        'table_name': 'query7_Salesman_wise_total_order_amt_and_incentive'
    },
    {
        'query': '''
            select i.ITEM_ID, i.ITEM_DESCRIPTION 
            from items_view i 
            where i.ITEM_ID not in (select item_id from order_details_view )
        ''',
        'table_name': 'query8_Items_not_sold_in_previous_month'
    },
    {
        'query': '''
            select c.CUST_ID, c.cust_name 
            from customers_view c 
            where c.CUST_ID not in (select s.CUST_ID  from ship_to_view s)
        ''',
        'table_name': 'query9_customer_whose_orders_not_shipped_in_previous_month'
    },
    {
        'query': '''
            select c.CUST_ID, c.cust_name 
            from customers_view c 
            join ship_to_view s  on (s.cust_id =c.cust_id)  
            where length(s.postal_code) !=6
        ''',
        'table_name': 'query10_customer_shipment_whose_address_not_filled_correctly_for_previous_month'
    }
]

if __name__ == '__main__':
    dbProperties = default_properties

    for query_info in queries:
        execute_query(spark, query_info['query'], query_info['table_name'], dbProperties)
