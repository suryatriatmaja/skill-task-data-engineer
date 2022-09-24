from datetime import datetime, timedelta
from itertools import count
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
import shutil
import psycopg2

default_args = {
    'owner': 'Surya Tri Atmaja',
    'email': ['suryatriatmaja87@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dags_scheduler_data_warehouse_eFishery',
    default_args=default_args,
    description='This dags for scheduler data warehouse',
    # schedule_interval='0 7 * * *',
    schedule_interval='@once',
    start_date=datetime(2022, 9, 24),
    catchup=False,
)

def start():
    print("Start process collect data to DWH")

def service_data(query):
    #loads
    try:
        connection = psycopg2.connect(user="postgres",
                                    password="postgres",
                                    host="localhost",
                                    port="5432",
                                    database="postgres")
        cursor = connection.cursor()
        cursor.execute(query)

        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into table")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def query_fact_order_accumulating():
    query = "insert into efishery_data_warehouse.fact_order_accumulating select id, orders.order_number, \
        invoices.invoice_number,payment_number, \
        quantity as total_order_quantity, \
        cast(usd_amount as decimal) as total_order_usd_amount, \
        cast(replace(cast((orders.date - invoices.date) as text),'-','') as integer) as order_to_invoice_lag_days, \
        cast(replace(cast((invoices.date - payments.date) as text),'-','') as integer) as invoice_to_payment_lag_days from customers \
        join orders on customers.id = orders.customer_id \
        join invoices on orders.order_number = invoices.order_number \
        join payments on payments.invoice_number = invoices.invoice_number \
        join order_lines on order_lines.order_number = orders.order_number"
    service_data(query)

def query_dim_date_payments():
    query = "insert into efishery_data_warehouse.dim_date with temp_payments as (SELECT * FROM payments WHERE EXTRACT(ISODOW FROM date) IN (6, 7))\
        select payment_number, date ,\
        cast(split_part(cast(date as text),'-','2') as integer) as month ,\
        (cast(split_part(cast(date as text),'-','2') as integer) - 1) / 3 + 1 as quarter ,\
        cast(split_part(cast(date as text),'-','1') as integer) as year ,\
        (case \
            when date in (select date from temp_payments) then true \
            when date not in (select date from temp_payments) then False \
        end) as is_weekend from payments"
    service_data(query)

def query_dim_date_invoices():
    query = "insert into efishery_data_warehouse.dim_date with temp_invoices as ( \
        SELECT * FROM invoices WHERE EXTRACT(ISODOW FROM date) IN (6, 7)) \
        select invoice_number, date ,\
        cast(split_part(cast(date as text),'-','2') as integer) as month ,\
        (cast(split_part(cast(date as text),'-','2') as integer) - 1) / 3 + 1 as quarter ,\
        cast(split_part(cast(date as text),'-','1') as integer) as year ,\
        (case \
            when date in (select date from temp_invoices) then true \
            when date not in (select date from temp_invoices) then False \
        end) as is_weekend from invoices"
    service_data(query)

def query_dim_date_orders():
    query = "insert into efishery_data_warehouse.dim_date with temp_orders as ( \
        SELECT * FROM orders WHERE EXTRACT(ISODOW FROM date) IN (6, 7)) \
        select order_number, date ,\
        cast(split_part(cast(date as text),'-','2') as integer) as month ,\
        (cast(split_part(cast(date as text),'-','2') as integer) - 1) / 3 + 1 as quarter ,\
        cast(split_part(cast(date as text),'-','1') as integer) as year ,\
        (case \
            when date in (select date from temp_orders) then true \
            when date not in (select date from temp_orders) then False \
        end) as is_weekend from orders "
    service_data(query)

def dim_customers():
    query = "insert into efishery_data_warehouse.dim_customers select id , name from customers"
    service_data(query)

def clean_table():
    query = "truncate table efishery_data_warehouse.dim_customers,efishery_data_warehouse.dim_date,efishery_data_warehouse.fact_order_accumulating"
    service_data(query)

def finish():
    print("Finish process collect data to DWH")
            
    

start_process = PythonOperator(task_id='start_process',
        python_callable=start, dag=dag)

clean_table_dwh = PythonOperator(task_id='clean_table_dwh',
        python_callable=clean_table, dag=dag)

dim_customers_process = PythonOperator(task_id='dim_customers_process',
        python_callable=dim_customers, dag=dag)

query_fact_order_accumulating_process = PythonOperator(task_id='query_fact_order_accumulating_process',
        python_callable=query_fact_order_accumulating, dag=dag)

query_dim_date_orders_process = PythonOperator(task_id='query_dim_date_orders_process',
        python_callable=query_dim_date_orders, dag=dag)

query_dim_date_invoices_process = PythonOperator(task_id='query_dim_date_invoices_process',
        python_callable=query_dim_date_invoices, dag=dag)

query_dim_date_payments_process = PythonOperator(task_id='query_dim_date_payments_process',
        python_callable=query_dim_date_payments, dag=dag)

finish_process = PythonOperator(task_id='finish_process',
        python_callable=finish, dag=dag)

start_process >> clean_table_dwh >> [dim_customers_process,query_fact_order_accumulating_process,query_dim_date_orders_process,query_dim_date_invoices_process,query_dim_date_payments_process] >> finish_process