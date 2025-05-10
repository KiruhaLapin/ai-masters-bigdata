# Домашняя работа №4: Spark ML — предсказание рейтинга по тексту обзора


## Описание
Задача: предсказать оценку товара (`overall`) по текстовому обзору (`reviewText`) и дополнительным признакам из JSON-файлов Amazon Reviews, используя Spark ML.

Данные:
- Тренировочный: `/datasets/amazon/train.json` (~20M записей)
- Тестовый: `/datasets/amazon/test83m.json` (~157M записей)

Целевая переменная: `overall` (рейтинг).  
Признаки: все остальные поля JSON (включая `vote`, `verified`, `reviewTime`, `reviewerID`, `asin`, `reviewerName`, `reviewText`, `summary`, `unixReviewTime`).

## Структура проекта (`projects/4`)
- `model.py` — определение Spark ML Pipeline в переменной `pipeline`.
- `train.py` — скрипт для обучения модели:
  Аргументы:
  1. `<TRAIN_PATH>` — путь в HDFS к тренировочному JSON.
  2. `<MODEL_PATH>` — директория для сохранения модели.
  

- `predict.py` — скрипт для инференса:
  Аргументы:
  1. `<MODEL_PATH>` — путь к сохранённой модели.
  2. `<TEST_PATH>` — путь к тестовому JSON.
  3. `<PRED_PATH>` — директория для сохранения предсказаний (CSV).


