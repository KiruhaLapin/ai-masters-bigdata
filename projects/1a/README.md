# Домашняя работа №1: Hadoop Streaming + sklearn инференс

## Описание
Практика распределённого инференса модели на основе `scikit-learn` с помощью Hadoop Streaming. Модель обучается на небольшом локальном семпле, а предсказания параллельно обрабатываются на кластере на полном датасете Criteo.

## Структура проекта
- `model.py` — определение конвейера (`Pipeline`) модели и полей:
  - `fields`, `numeric_features`, `categorical_features`
- `train.py` — обучение модели и сохранение в `1a.joblib`
- `train.sh` — оболочка для запуска `train.py`
- `predict.py` — загрузка `1a.joblib` и вывод предсказаний в `stdout`
- `predict.sh` — запуск инференса как Hadoop Streaming job
- `filter_cond.py` — функция `filter_cond(line_dict)` с условием `20 < if1 < 40`
- `filter.py` — маппер для фильтрации записей
- `filter.sh` — запуск фильтрации через Hadoop Streaming
- `filter_predict.sh` — объединённый map-reduce: фильтрация на стадии map и предсказания на стадии reduce
- `local_scorer.py` — локальный расчёт метрики LogLoss (использует `sklearn`)

## Запуск
1. **Обучение**
   ```bash
   cd ai-masters-bigdata
   projects/1a/train.sh 1a /home/users/datasets/criteo/train1000.txt
   ```

2. **Инференс (HDFS)**
   ```bash
   hdfs dfs -rm -r -f -skipTrash predicted.csv
   projects/1a/predict.sh projects/1a/predict.py,1a.joblib /datasets/criteo/train-with-id.txt predicted.csv predict.py
   ```

3. **Фильтрация**
   ```bash
   projects/1a/filter.sh projects/1a/filter.py /datasets/criteo/train-with-id.txt filtered.txt filter.py
   ```

4. **Фильтрация + предсказания**
   ```bash
   projects/1a/filter_predict.sh projects/1a/filter.py,projects/1a/predict.py,projects/1a/filter_cond.py,1a.joblib,projects/1a/model.py /datasets/criteo/train-with-id.txt pred_with_filter filter.py predict.py
   ```
