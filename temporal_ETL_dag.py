# Imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests
import json
from requests.structures import CaseInsensitiveDict
from datetime import datetime, timedelta

import sqlite3


# Argumentos de Apache Airflow
default_args = {
    'owner': 'Lander Bonilla Viana',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email': ['bonillalander@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG (
    dag_id= 'Temporal_ETL_dag', # nombre del DAG, debe de ser diferente
    default_args= default_args,
    description='ETL_Temporal_PFG',
    tags=['PFG', 'temporal'],
    schedule_interval="0 10 * * *", # Intervaluo de ejecución
) as dag:

    # Fecha actual
    def fecha_hoy():
        fecha = datetime.today()
        dia = str(fecha.day)
        mes = fecha.month
        anno = str(fecha.year)
        if mes < 10:
            mes = "0" + str(mes)
        return anno, mes, dia


    # Extracción de los datos
    def extract_data(**kwargs):
        ti = kwargs['ti']
        anno, mes, dia = ti.xcom_pull(task_ids='fecha_hoy')

        # Dirección de la API y token JWT
        url = "https://api.euskadi.eus/euskalmet/weather/regions/basque_country/forecast/at/"+ anno + "/" + mes + "/" + dia + "/for/" + anno + mes + dia
        JWT = "eyJhbGciOiJSUzI1NiJ9.eyJhdWQiOiJtZXQwMS5hcGlrZXkiLCJpc3MiOiJUZWNuYWxpYSIsImV4cCI6MTY4Mzg3NTE1OCwidmVyc2lvbiI6IjEuMC4wIiwiaWF0IjoxNjUyMzM5MTU4LCJlbWFpbCI6ImxhbmRlci5ib25pbGxhQG9wZW5kZXVzdG8uZXMifQ.rl0VjQp3ACDthF0Z2EDfo4kLU8vLcHwFqlSESNPCzRNHxHGYJWpgRB9X0q4PHC6pcgk91tcg1XGJ47ZeJ0lN0ri3E1qvCzvnX22MC2XgdgFCYUOvSRa_t7rgM-RlcVscT3lBZtiBpxIMilWAcMVkflckUk1iFbynr9tSuZJjl_aZSS5unCva09rLKYIi3HUaFRj8nUbcHGUH47w_C3h973a2ExCUjjWR8d4BeyU4HMFZZPsCoe3xKs7hbCi5d5w1o4OtBQ_-5y9Pb2ni_JgXnhX5XGxqvDMGGv-YBNYGuJ_aS-85p4vBJ67kk9JEyH0jzlcvcjxAmnlT_H03uy8Vfw"

        # Llamada a ala API
        headers = CaseInsensitiveDict()
        headers["Accept"] = "application/json"
        headers["Authorization"] = "Bearer " + JWT
        response = requests.get(url, headers=headers)

        # Repsuesta de la API
        print(response.status_code)
        df = response.json()
        with open("/opt/airflow/dags/Datos_ETL/temporal_sin_limpiar.json", 'a') as file:
            json.dump(df, file, indent=4)
        return df

    # Transformación de los datos, quedarse solo con la localización y sus temp max y min
    def transform_data(**kwargs):
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_data')
        datos_limpios=[]
        for data in df["citiesTemperatureRange"]:
            if data["locationId"]=="Vitoria-Gasteiz" or data["locationId"]=="Donostia/San Sebasti\u00e1n" or data["locationId"]=="Bilbao":
                ciudad={}
                ciudad["localizacion"] = data["locationId"]
                for temperatura in data["temperature"]:
                    if temperatura == "min":
                        ciudad["min"] = data["temperature"][temperatura]
                    elif temperatura == "max":
                        ciudad["max"] =data["temperature"][temperatura]
                datos_limpios.append(ciudad)
        return datos_limpios


    # Cargar datos en un archivo SQLite
    def load_data(**kwargs):
        ti = kwargs['ti']
        datos_limpios = ti.xcom_pull(task_ids='transform_data')
        DATABASE_LOCATION = "/opt/airflow/dags/Datos_ETL/temporal.sqlite"
        fecha = datetime.today()
        fecha = fecha.strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DATABASE_LOCATION)
        cursor = conn.cursor()

        sql_query_table = """
        CREATE TABLE IF NOT EXISTS temporal(
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            localizacion VARCHAR(200),
            min REAL,
            max REAL,
            fecha TEXT
        )
        """

        cursor.execute(sql_query_table)
        print("Opened database successfully")
        
        
        for c in datos_limpios:
            cursor.execute("INSERT INTO temporal (localizacion, min, max, fecha) values (?,?,?,?)", (c["localizacion"], c["min"], c["max"], datetime.strptime(fecha,"%Y-%m-%d %H:%M:%S")))
            conn.commit()
            print("Dato subido")
        conn.close()
        print("Close database successfully")
        datos_limpios.clear()


    # Operadores de Apache Airflow, deben tener task_id diferentes
    fecha_etl = PythonOperator(
        task_id='fecha_hoy',
        python_callable=fecha_hoy,
        dag=dag,
    )

    extract_data_etl = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
    )

    transform_data_etl = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )

    load_data_etl = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag,
    )

    # Orden de ejecución de los operadores creados anteriormente
    fecha_etl >> extract_data_etl >> transform_data_etl >> load_data_etl