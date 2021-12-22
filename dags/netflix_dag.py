
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from netflix_etl import create_mysql_database, create_mysql_table, insert_mysql_table, mysql_to_json
from structured_data import structured
from unzip import unzip
from structured_data import structured


default_args = {
    'owner': 'airflow',
    'mysql_conn_id': 'mysql_os',
    'retry_delay': timedelta(minutes=1),
    'retries': 1,
    'depends_on_past': False
}

with DAG(dag_id="netflix_dag",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         catchup=False,
         default_args=default_args) as dag:

    unzip = PythonOperator(
        task_id="download_csv",
        python_callable=unzip,
        dag=dag
    )

    create_database_task = PythonOperator(
        task_id="create_database",
        python_callable=create_mysql_database,
        dag=dag
    )

    drop_table_mysql_task = MySqlOperator(
        task_id='drop_table',
        mysql_conn_id='mysql_os',
        # database="netflix_mysql_database",
        sql="""DROP TABLE IF EXISTS netflix_mysql_database.netflix_mysql_table;""",
        dag=dag
    )

    create_table_mysql_task = PythonOperator(
        task_id="create_table",
        python_callable=create_mysql_table,
        dag=dag
    )

    insert_table_mysql_task = PythonOperator(
        task_id="insert_table",
        python_callable=insert_mysql_table,
        dag=dag
    )

    mysql_to_json_task = PythonOperator(
        task_id="my_sql_json",
        python_callable=mysql_to_json,
        dag=dag
    )

    show_graph_task = PythonOperator(
        task_id="show_graph",
        python_callable=structured,
        dag=dag
    )

    unzip >> create_database_task >> drop_table_mysql_task >> create_table_mysql_task >> insert_table_mysql_task >> mysql_to_json_task
    unzip >> create_database_task >> drop_table_mysql_task >> create_table_mysql_task >> insert_table_mysql_task >>show_graph_task
