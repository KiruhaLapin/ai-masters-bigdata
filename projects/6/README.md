# Домашняя работа №6: Airflow DAG для Sentiment Analysis


## Описание
Построить Airflow DAG, реализующий полный pipeline анализа настроения (label) отзывов из JSON Amazon:
1. Feature engineering (Spark) на тренировочных данных  
2. Обучение модели (scikit-learn)  
3. Feature engineering (Spark) на тестовых данных  
4. Инференс (Spark + pandas_udf) с сохранённой моделью


