# Домашняя работа №2: Hive + Criteo инференс

## Описание
Повторяем задачу предсказания вероятности клика (Criteo Display Advertising challenge), но весь процесс выполняем через Apache Hive.

## Структура проекта (`projects/2a`)
- `create_test.hql` — создание внешней временной таблицы `hw2_test` для тестового датасета `/datasets/criteo/testdir`.
- `create_pred.hql` — создание управляемой таблицы `hw2_pred` для предсказаний (столбцы `id`, `prediction`), с `LOCATION '<login>_hw2_pred'`.
- `filter_predict.hql` — вставка в `hw2_pred` через `INSERT ... SELECT` с фильтрацией `20 < if1 < 40` и предсказаниями модели.
- `select_out.hql` — выгрузка содержимого `hw2_pred` в текстовый файл `'<login>_hiveout'` в HDFS.

Дополнительно:
- `model.py` — определение конвейера (`Pipeline`) модели и полей (`fields`, `numeric_features`, `categorical_features`).
- `train.py`, `train.sh` — обучение модели и сохранение в `2a.joblib`.
- `predict.py` — загрузка `2a.joblib` и вывод предсказаний (используется в `filter_predict.hql`).


