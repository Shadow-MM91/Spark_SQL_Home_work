# Использовать в терминале Linux spark-submit python_work.py для запуска

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, round, max, lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
sc = SparkContext('local')
spark = SparkSession(sc)

# Задние №1
"""
Выберите 15 стран с наибольшим процентом переболевших на 31 марта 
(в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)
"""
df = spark.read.option('interSchema','true').option('header','true').csv('cowid-covid-data.csv')
df_1 = df.filter( (col('date') == '2021-03-31') )\
  .withColumn("int_total_cases", col("total_cases").cast('float'))\
  .withColumn("int_population", col("population").cast('float'))\
  .withColumn("recovered", (col('int_total_cases') * 100 / col('int_population')))\
  .select('iso_code','location',(round('recovered', 2)).alias('recovered')).orderBy(col("recovered").desc()).limit(15)
df_1.coalesce(1).write.csv('home_work_1.csv', header=True)

# Задание №2
"""
Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию
(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
"""
df_2 = df.filter( (col('date') > lit('2021-03-24')) & (col('date') <= lit('2021-03-31')) ) \
  .filter(~F.col("location").isin(['World','Europe','European Union','Asia','South America','North America','Africa']))\
  .withColumn("new_cases", col("new_cases").cast(IntegerType()))\
  .groupby('location').agg(max("new_cases").alias("new_cases"))
 covid_2 = df.select('date','location','new_cases')\
  .filter( (col('date') > lit('2021-03-24')) & (col('date') <= lit('2021-03-31')) )
new_covid = covid_2.join( df_2,  on=['location','new_cases'] , how='inner')
new_covid = new_covid.withColumn("new_cases", col("new_cases").cast(IntegerType()))
new_covid = new_covid.select('date','location','new_cases').orderBy(col('new_cases').desc()).limit(10)
new_covid.coalesce(1).write.csv('home_work_2.csv', header=True)

#Задание №3
"""
Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. 
(например: в россии вчера было 9150 , сегодня 8763, итог: -387) 
(в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)
"""
df_3 = df.withColumn('yesterday', F.lag('new_cases',1).over(Window.partitionBy('location').orderBy('date')))\
.filter( (col('date') >= '2021-03-25') & (col('date') <= '2021-03-31') )
df_3 = df_3.withColumn('diffirent', col('new_cases') - col('yesterday'))
df_3 = df_3.filter(col('location') == 'Russia').select('date','location','new_cases','yesterday','diffirent')
df_3.coalesce(1).write.csv('home_work_3.csv', header=True)
