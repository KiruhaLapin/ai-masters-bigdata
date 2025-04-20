from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime
'''
pyspark_python = "/opt/conda/envs/dsenv/bin/python"

with DAG(
    dag_id='KiruhaLapin_dag',
    start_date=datetime(2025, 4, 20),
    schedule=None,
    catchup=False,
    tags=['hw6']
) as dag:

    base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

    # Feature engineering для тренировочных данных
    feature_eng_train_task = SparkSubmitOperator(
        task_id='feature_eng_train_task',
        application=f"{base_dir}/spark_feature_eng.py",
        conn_id='spark_default',
        spark_binary="/usr/bin/spark3-submit",
        env_vars={
            "PYSPARK_PYTHON": "/opt/conda/envs/dsenv/bin/python",
        },
        application_args=[
            '--path-in', '/datasets/amazon/amazon_extrasmall_train.json',
            '--path-out', 'KiruhaLapin_train_out'
        ]
    )

    # Загрузка обработанных тренировочных данных в локальную ФС
    download_train_task = BashOperator(
        task_id='download_train_task',
        bash_command=(
            #f"hdfs dfs -get {base_dir}/KiruhaLapin_train_out {base_dir}/KiruhaLapin_train_out_local"
            f"hdfs dfs -get KiruhaLapin_train_out {base_dir}/KiruhaLapin_train_out_local"
        ),
    )

    # Обучение модели
    train_task = BashOperator(
        task_id='train_task',
        bash_command=(
            f"{pyspark_python} {base_dir}/train_sklearn.py "
            f"--train-in {base_dir}/KiruhaLapin_train_out_local "
            f"--sklearn-model-out {base_dir}/6.joblib"
        ),
    )

    # Проверка наличия модели
    model_sensor = FileSensor(
        task_id='model_sensor',
        filepath=f"{base_dir}/6.joblib",
        poke_interval=30,
        timeout=10,
    )

    # Feature engineering для тестовых данных
    feature_eng_test_task = SparkSubmitOperator(
        task_id='feature_eng_test_task',
        application=f"{base_dir}/spark_feature_eng.py",
        conn_id='spark_default',
        application_args=[
            '--path-in', '/datasets/amazon/amazon_extrasmall_test.json',
            #'--path-out', f"{base_dir}/KiruhaLapin_test_out"
            '--path-out', f"KiruhaLapin_test_out"
        ],
        env_vars={'PYSPARK_PYTHON': pyspark_python},
    )

    # Предсказание
    predict_task = SparkSubmitOperator(
        task_id='predict_task',
        application=f"{base_dir}/spark_inference.py",
        conn_id='spark_default',
        spark_binary="/usr/bin/spark3-submit",
        application_args=[
            #'--test-in', f"{base_dir}/KiruhaLapin_test_out",
            #'--pred-out', f"{base_dir}/KiruhaLapin_hw6_prediction",
            '--test-in', f"KiruhaLapin_test_out",
            '--pred-out', f"KiruhaLapin_hw6_prediction",
            '--sklearn-model-in', f"{base_dir}/6.joblib"
        ],
        env_vars={'PYSPARK_PYTHON': pyspark_python},
    )

    feature_eng_train_task >> download_train_task >> train_task >> model_sensor >> feature_eng_test_task >> predict_task
'''

import json
import os

#from textwrap import dedent
from airflow.operators.bash import BashOperator

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG


#AIRFLOW_HOME=os.environ.get("AIRFLOW_HOME", 
#                            f'{os.environ["HOME"]}/airflow')

with DAG(
    'KiruhaLapin_dag',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    description='sample hello world dag',
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 4, tz="UTC"),
    catchup=False,
    tags=['aimasters'],
) as dag:
    dag.doc_md = __doc__

    hello_world_task = BashOperator(
         task_id='hello_world_task',
         bash_command = f'echo Hello World'
    )

    hello_user_task = BashOperator(
         task_id='hello_user_task', 
         bash_command = f'echo Hello KiruhaLapin'
    )

    hello_user_task >> hello_world_task
