from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime


#lol
with DAG(
    dag_id='KiruhaLapin_dag',
    start_date=datetime(2025, 4, 20),
    schedule=None,
    catchup=False,
    tags=['hw6']
) as dag:

    base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

    # Общие настройки для SparkSubmitOperator
    spark_defaults = {
        "spark_binary": "/usr/bin/spark3-submit",
        "conn_id": "spark_default",
        "env_vars": {
            "PYSPARK_PYTHON": "/opt/conda/envs/dsenv/bin/python",
            "PYSPARK_DRIVER_PYTHON": "/opt/conda/envs/dsenv/bin/python",
            "SPARK_HOME": "/usr/lib/spark3"  # Проверьте точный путь!
        },
        "conf": {
            "spark.yarn.queue": "default",
            "spark.submit.deployMode": "client"
        }
    }

    # Feature engineering для тренировочных данных
    feature_eng_train_task = SparkSubmitOperator(
        task_id='feature_eng_train_task',
        application=f"{base_dir}/spark_feature_eng.py",
        application_args=[
            '--path-in', '/datasets/amazon/amazon_extrasmall_train.json',
            '--path-out', 'KiruhaLapin_train_out'  # Относительный путь в HDFS
        ],
        **spark_defaults
    )

    download_train_task = BashOperator(
        task_id='download_train_task',
        bash_command=(
            # 1. Создать локальную директорию
            f"mkdir -p {base_dir}/KiruhaLapin_train_out_local && "
            # 2. Скопировать ВСЕ файлы из HDFS-директории
            f"hdfs dfs -get hdfs:///user/KiruhaLapin/KiruhaLapin_train_out/* {base_dir}/KiruhaLapin_train_out_local/"
        ),
    )

    # Обучение модели
    train_task = BashOperator(
        task_id='train_task',
        bash_command=(
            f"/opt/conda/envs/dsenv/bin/python {base_dir}/train_sklearn.py "
            f"--train-in {base_dir}/KiruhaLapin_train_out_local "
            f"--sklearn-model-out {base_dir}/6.joblib"
        ),
    )

    # Сенсор модели
    model_sensor = FileSensor(
        task_id='model_sensor',
        filepath=f"{base_dir}/6.joblib",
        poke_interval=30,
        timeout=600  # Увеличьте таймаут до 10 минут
    )

    # Feature engineering для тестовых данных
    feature_eng_test_task = SparkSubmitOperator(
        task_id='feature_eng_test_task',
        application=f"{base_dir}/spark_feature_eng.py",
        application_args=[
            '--path-in', '/datasets/amazon/amazon_extrasmall_test.json',
            '--path-out', 'KiruhaLapin_test_out'
        ],
        **spark_defaults
    )

    # Предсказание
    predict_task = SparkSubmitOperator(
        task_id='predict_task',
        application=f"{base_dir}/spark_inference.py",
        application_args=[
            '--test-in', 'KiruhaLapin_test_out',
            '--pred-out', 'KiruhaLapin_hw6_prediction',
            '--sklearn-model-in', f"{base_dir}/6.joblib"
        ],
        **spark_defaults
    )

    feature_eng_train_task >> download_train_task >> train_task >> model_sensor >> feature_eng_test_task >> predict_task


