""" Решение задачи 8 """

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

import Filer


def task_8():
    """  Задача 8.  Работа с файлом submissions_data_train.csv """

    # -----------------------------------------------------------------------------------------
    #   Задача 8(1/2). Для каждого пользователя найдите такой шаг, который он не смог решить,
    #                  и после этого не пытался решать другие шаги.
    # -----------------------------------------------------------------------------------------

    # исходный файл
    input_submissions = "input/less_3/submissions_data_train.csv"

    spark = SparkSession \
        .builder \
        .appName("task 8") \
        .master("local") \
        .getOrCreate()

    dataset_submissions = spark \
        .read \
        .option("header", True) \
        .csv(input_submissions)

    dataset_submissions = dataset_submissions.withColumn("timestamp", dataset_submissions["timestamp"].cast(LongType()))

    dataset_correct = dataset_submissions \
        .where("submission_status == 'correct'") \
        .select("user_id", "step_id") \
        .distinct()

    dataset_last_action = dataset_submissions \
        .groupBy("user_id") \
        .agg(F.max("timestamp")) \
        .withColumnRenamed("user_id", "user_id_2") \
        .withColumnRenamed("max(timestamp)", "timestamp_last_act")

    dataset_task_8_part_1 = dataset_last_action \
        .join(dataset_submissions,
              (dataset_last_action["user_id_2"] == dataset_submissions["user_id"]) &
              (dataset_last_action["timestamp_last_act"] == dataset_submissions["timestamp"]),
              how="inner") \
        .where("submission_status = 'wrong'") \
        .select("user_id", "step_id") \
        .distinct()

    # вычетаем тех, кто решил этот шаг раньше
    dataset_task_8_part_1 = dataset_task_8_part_1.exceptAll(dataset_correct)

    output = "output/less_4/task_8_part_1"
    Filer.FileManager.save(dataset_task_8_part_1, output)

    # -----------------------------------------------------------------------------------------
    #   Задача 8(2/2). Найдите id шага, который стал финальной точкой практического обучения
    #                  на курсе для максимального числа пользователей.
    # -----------------------------------------------------------------------------------------

    dataset_task_8_part_2 = dataset_task_8_part_1 \
        .groupBy("step_id") \
        .agg(F.count("user_id")) \
        .orderBy(F.desc("count(user_id)")) \
        .select("step_id") \
        .limit(1)

    output = "output/less_4/task_8_part_2"
    Filer.FileManager.save(dataset_task_8_part_2, output)
