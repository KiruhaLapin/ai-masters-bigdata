#!/bin/bash

# Получаем аргументы
FILES_TO_SEND=$1  # Файлы (скрипты и модель)
INPUT_DATASET=$2  # Входные данные на HDFS
OUTPUT_DATASET=$3 # Выходной файл на HDFS
MAPPER_SCRIPT=$4  # Скрипт фильтрации (filter.py)
REDUCER_SCRIPT=$5 # Скрипт предсказания (predict.py)

# Запуск map-reduce задачи
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar\
    -files $FILES_TO_SEND \
    -mapper "python3 $MAPPER_SCRIPT" \
    -reducer "python3 $REDUCER_SCRIPT" \
    -input $INPUT_DATASET \
    -output $OUTPUT_DATASET

