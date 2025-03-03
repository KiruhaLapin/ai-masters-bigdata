#!/bin/bash

FILES_TO_SEND=$1
INPUT_DATASET=$2  # Входные данные (на HDFS)
OUTPUT_DATASET=$3 # Выходной файл
SCRIPT_TO_RUN=$4  # Скрипт предсказаний

# Запуск map-reduce задачи
hadoop jar $HADOOP_STREAMING \
    -files $FILES_TO_SEND \
    -mapper "python3 $SCRIPT_TO_RUN" \
    -input $INPUT_DATASET \
    -output $OUTPUT_DATASET

