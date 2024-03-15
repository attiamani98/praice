import psycopg2
from psycopg2 import sql
import datetime
import os



connection = psycopg2.connect(os.environ["database_url"])

cursor = connection.cursor()


execution_date = datetime.datetime.now()

for category, category_data in products.items():
    for product, product_data in category_data['products'].items():
        batch_name = product
        batch_id = product_data['id']
        sell_by = product_data['sell_by']
        
        # Check if the product already exists in the database
        select_query = sql.SQL("SELECT COUNT(*) FROM batchs WHERE batch_name = %s AND product = %s")
        cursor.execute(select_query, (batch_name, category))
        count = cursor.fetchone()[0]
        
        # If the product doesn't exist, insert it into the database
        if count == 0:
            insert_query = sql.SQL("INSERT INTO batchs (batch_name, product, batch_id, sell_by, execution_time) VALUES (%s, %s, %s, %s, %s)")
            cursor.execute(insert_query, (batch_name, category, batch_id, sell_by, execution_date))

connection.commit()
cursor.close()
connection.close()
