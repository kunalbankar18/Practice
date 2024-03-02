import pandas as pd
import mysql.connector
from mysql.connector import Error
import MySQLdb 

# Function to create a MySQL connection
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='sample',
            user='root',
            password='2311'
        )
        if connection:
            print(f"Connected to MySQL Server: {connection.get_server_info()}")
            return connection
    except MySQLdb.Error as e:
        print(f"Error: {e}")
        return None

# Function to perform ETL
def etl():
    # Extract data from CSV
    csv_file = 'city.csv'
    df = pd.read_csv(csv_file)

    # Transform data if needed (you can add your transformation logic here)

    # Load data into MySQL
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()

            # Assuming your MySQL table is named 'your_table_name'
            table_name = 'city'

            # Create the table if it doesn't exist
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (srno INT, city VARCHAR(255));"
            cursor.execute(create_table_query)

            # Insert data into the table
            for index, row in df.iterrows():
                # Use parameterized query to avoid SQL injection
                insert_query = f"INSERT INTO {table_name} (srno, city) VALUES (%s, %s);"
                data = (row['srno'], row['city'])
                cursor.execute(insert_query, data)


            # Commit the changes
            connection.commit()

            print("ETL process completed successfully.")

        except Error as e:
            print(f"Error: {e}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection closed.")
            else:
                print("MySQL connection is not open.")

if __name__ == "__main__":
    etl()
