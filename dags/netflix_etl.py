
import mysql
import pandas as pd
import mysql.connector
from mysql.connector import (connection)


def create_mysql_database():
    conn = connection.MySQLConnection(user='zipcoder3', password='zipcode0', host='localhost')
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS netflix_mysql_database")
    cursor.execute('CREATE DATABASE netflix_mysql_database')
    cursor.execute("USE netflix_mysql_database")
    conn.commit()


def create_mysql_table():
    conn = connection.MySQLConnection(user='zipcoder3', password='zipcode0', host='localhost',database='netflix_mysql_database')
    cursor = conn.cursor()
    # cursor.execute('DROP TABLE IF EXISTS netflix_mysql_table')
    cursor.execute("CREATE TABLE netflix_mysql_table(show_id VARCHAR(200), type VARCHAR(200), title VARCHAR(200), director LONGTEXT, cast LONGTEXT, country VARCHAR(200), date_added VARCHAR(200), release_year VARCHAR(200), rating VARCHAR(200), duration VARCHAR(200), listed_in VARCHAR(200), description LONGTEXT)")
    conn.commit()
    conn.close()


def insert_mysql_table():
    conn = connection.MySQLConnection(user='zipcoder3', password='zipcode0', host='localhost')
    cursor = conn.cursor()
    netflix_data = pd.read_csv('/Users/cantekinefe/airflow/airflow_home/dags/netflix_titles.csv', index_col=False)
    netflix_data = netflix_data.where(pd.notnull(netflix_data), None)
    for i, row in netflix_data.iterrows():
        sql_insert = "INSERT INTO netflix_mysql_database.netflix_mysql_table  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql_insert, tuple(row))
    conn.commit()
    conn.close()

def mysql_to_json():
    conn = mysql.connector.connect(user='zipcoder3', password='zipcode0', host='localhost', database='netflix_mysql_database')
    if conn.is_connected():
        query = "SELECT * FROM netflix_mysql_table;"
        pull_df = pd.read_sql_query(query, conn)
        # return pull_df
        conn.close()
    pull_df.to_json('/Users/cantekinefe/airflow/airflow_home/dags/netflix.json')

