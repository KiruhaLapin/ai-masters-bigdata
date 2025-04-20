from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='KiruhaLapin_dag',
    start_date=datetime(2025, 4, 20),
    schedule=None,
    catchup=False,
    tags=['hw6']
) as dag:

    # base_dir подставится из dag_run.conf, либо будет пустой строкой
    base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'

    spark_defaults = {
        "spark_binary": "/usr/bin/spark3-submit",
        "conn_id": "spark_default",
        "env_vars": {
            "PYSPARK_PYTHON": "/opt/conda/envs/dsenv/bin/python",
            "PYSPARK_DRIVER_PYTHON": "/opt/conda/envs/dsenv/bin/python",
            "SPARK_HOME": "/usr/lib/spark3"
        },
        "conf": {
            "spark.yarn.queue": "default",
            "spark.submit.deployMode": "client"
        }
    }

    # 1) Feature engineering тренировочных данных (в HDFS: KiruhaLapin_train_out)
    feature_eng_train_task = SparkSubmitOperator(
        task_id='feature_eng_train_task',
        application=f"{base_dir}/spark_feature_eng.py",
        application_args=[
            '--path-in', '/datasets/amazon/amazon_extrasmall_train.json',
            '--path-out', 'KiruhaLapin_train_out'
        ],
        **spark_defaults
    )

    # 2) Скачиваем из HDFS в локалку base_dir/KiruhaLapin_train_out_local
    download_train_task = BashOperator(
        task_id='download_train_task',
        bash_command=(
            f"mkdir -p {base_dir}/KiruhaLapin_train_out_local && "
            f"hdfs dfs -get KiruhaLapin_train_out/* {base_dir}/KiruhaLapin_train_out_local/"
        )
    )

    # 3) Обучаем sklearn-модель, сохраняем в base_dir/6.joblib
    train_task = BashOperator(
        task_id='train_task',
        bash_command=(
            f"/opt/conda/envs/dsenv/bin/python {base_dir}/train_sklearn.py "
            f"--train-in {base_dir}/KiruhaLapin_train_out_local "
            f"--sklearn-model-out {base_dir}/6.joblib"
        )
    )

    # 4) Ждём файл модели в base_dir
    model_sensor = FileSensor(
        task_id='model_sensor',
        filepath=f"{base_dir}/6.joblib",
        poke_interval=30,
        timeout=60 * 10,  # 10 минут
        mode='poke'
    )

    # 5) Feature engineering тестовых данных в HDFS: KiruhaLapin_test_out
    feature_eng_test_task = SparkSubmitOperator(
        task_id='feature_eng_test_task',
        application=f"{base_dir}/spark_feature_eng.py",
        application_args=[
            '--path-in', '/datasets/amazon/amazon_extrasmall_test.json',
            '--path-out', 'KiruhaLapin_test_out'
        ],
        **spark_defaults
    )

    # 6) Предсказание: читаем модель из base_dir, пишем HDFS: KiruhaLapin_hw6_prediction
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

    feature_eng_train_task \
        >> download_train_task \
        >> train_task \
        >> model_sensor \
        >> feature_eng_test_task \
        >> predict_task




