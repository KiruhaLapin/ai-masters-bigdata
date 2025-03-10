-- Создаем базу данных, если она еще не существует
CREATE DATABASE IF NOT EXISTS KiruhaLapin_checker;
USE KiruhaLapin_checker;

-- Удаляем таблицу hw2_test, если она существует
DROP TABLE IF EXISTS hw2_test;

-- Создаем таблицу hw2_test из файла create_test.hql
SOURCE projects/2a/create_test.hql;

-- Описываем структуру таблицы hw2_test
DESCRIBE hw2_test;

-- Считаем количество записей в таблице hw2_test
SELECT COUNT(id) FROM hw2_test;

-- Удаляем таблицу hw2_pred, если она существует
DROP TABLE IF EXISTS hw2_pred;

-- Создаем таблицу hw2_pred из файла create_pred.hql
SOURCE projects/2a/create_pred.hql;

-- Описываем структуру таблицы hw2_pred
DESCRIBE hw2_pred;

-- Заполняем таблицу hw2_pred с использованием скрипта filter_predict.hql
SOURCE projects/2a/filter_predict.hql;

-- Считаем количество записей в таблице hw2_pred
SELECT COUNT(id) FROM hw2_pred;

-- Выполняем финальный запрос из файла select_out.hql
SOURCE projects/2a/select_out.hql;
