from datetime import datetime
from retrieve_data import get_data
from transform_data import transform
from datetime import date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator

# make it run mon-friday at 8pm
first_dag = DAG(
    dag_id='first_airflow_dag',
    description='sample dag',
    schedule= '0 19 * * 1-5', # run at 19:00, Monday through Friday UTC timezone
    start_date=datetime(year=2022, month=12, day=23),
)

retrieve_data = PythonOperator(
    task_id = 'retrieve_data',
    python_callable = get_data,
    dag = first_dag
)

transform_data = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform,
    dag = first_dag
)

t_timestamp = date.today()
t_string = t_timestamp.strftime('%Y-%m-%d')
options_table_name = "aapl_options_{}".format(t_string)
options_table_name = options_table_name.replace('-', '_') #replace - with _ because - is special character in mysql
cleaned_combined_file_name = "combined_data_{}.csv".format(t_string)
save_file_location = "/home/bkim/processed_data_dir/" + cleaned_combined_file_name

create_table = MySqlOperator(
    task_id = 'create_table',
    mysql_conn_id = 'mysql_root_connection',
    sql = 'CREATE TABLE IF NOT EXISTS {} (expirationDate VARCHAR(15) NOT NULL,'\
          'daysToExpiration INT NOT NULL, stock_price FLOAT(10) NOT NULL, bid FLOAT(10) NOT NULL,'\
          'ask FLOAT(10) NOT NULL, strikePrice FLOAT(10) NOT NULL, totalVolume INT NOT NULL,'\
          'volatility FLOAT(10) NOT NULL, delta FLOAT(10) NOT NULL, gamma FLOAT(10) NOT NULL,'\
          'theta FLOAT(10) NOT NULL, vega FLOAT(10) NOT NULL);'.format(options_table_name)
)

insert_data = BashOperator(
    task_id = 'insert_data',
    bash_command = 'bash /home/bkim/insert_data.sh {} {}'.format(save_file_location, options_table_name),
    dag = first_dag
)



retrieve_data >> transform_data >> create_table >> insert_data