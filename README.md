# ETL-pipline-karpov.courses-

## В файле etl_davydenko.py представлен код DAG в airflow:
## - выгружает данные за предыдущий день из 2-х баз (feed_actions и message_actions)
## - объединяет 2 df
## - преобразовывает данные по срезам (os, gender, age)
## - загружает получившиеся данные в таблицу в clickhouse

## скриншот итоговой таблицы в clickhouse:
![Screenshot_etl](https://user-images.githubusercontent.com/122218714/211315263-dbd86abb-c99e-43d6-8598-ba76ff17cbc3.png)
