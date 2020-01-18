""" Решение задачь с 4 по 7 """

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

import Filer


def task_4():
    """  Задача 4. Рассчитать среднее время исполнения заявки
                   в разрезе страховых компаний
    """
    # исходный файл
    input = "input/less_3/data_task_1.csv"

    spark = SparkSession \
        .builder \
        .appName("task 4") \
        .master("local") \
        .getOrCreate()

    dataset = spark \
        .read \
        .option("header", True) \
        .csv(input)

    dataset = dataset.withColumn("total_hrs_in_status", dataset["total_hrs_in_status"].cast(FloatType()))

    dataset_task_4 = dataset \
        .groupBy("insurer_nm", "claim_id") \
        .sum("total_hrs_in_status") \
        .groupBy("insurer_nm") \
        .avg("sum(total_hrs_in_status)") \
        .withColumnRenamed("avg(sum(total_hrs_in_status)", "avg(total_hrs_in_status)_by_person")

    output = "output/less_3/task_4"
    Filer.FileManager.save(dataset_task_4, output)


def task_5_6_7():
    """  Задачи с 5 по 7. Работа с файлами event_data_train.csv и
         submissions_data_train.csv
    """

    # ----------------------------------------------------------------------------------
    #   Задача 5. На основании набора (event_data_train.csv) найти всех пользователей,
    #             которые полностью прошли курс
    # ----------------------------------------------------------------------------------

    # исходный файл
    input_event = "input/less_3/event_data_train.csv"

    spark = SparkSession \
        .builder \
        .appName("task 5") \
        .master("local") \
        .getOrCreate()

    dataset_event = spark \
        .read \
        .option("header", True) \
        .csv(input_event)

    # всего шагов в курсе
    steps = dataset_event \
        .select("step_id") \
        .distinct() \
        .count()

    dataset_task_5 = dataset_event \
        .select("step_id", "user_id") \
        .where("action = 'passed'") \
        .groupBy("user_id") \
        .count() \
        .where(f"count == {steps}") \
        .select("user_id")

    output = "output/less_3/task_5"
    Filer.FileManager.save(dataset_task_5, output)

    # ----------------------------------------------------------------------------------
    #   Задача 6. На основании набора (submissions_data_train.csv) найти сколько задач
    #             успешно решили пользователи, которые прошли курс полностью
    # ----------------------------------------------------------------------------------

    input_submissions = "input/less_3/submissions_data_train.csv"

    dataset_submissions = spark \
        .read \
        .option("header", True) \
        .csv(input_submissions) \
        .withColumnRenamed("user_id", "user_id_2") \
        .withColumnRenamed("step_id", "step_id_2")

    dataset_task_6 = dataset_submissions \
        .where("submission_status = 'correct'") \
        .select("step_id_2", "submission_status", "user_id_2") \
        .distinct() \
        .join(dataset_task_5,
              dataset_submissions["user_id_2"] == dataset_task_5["user_id"],
              how="inner") \
        .select("submission_status", "user_id") \
        .groupBy("user_id") \
        .count() \
        .withColumnRenamed("count", "number_of_solved_task")

    output = "output/less_3/task_6"
    Filer.FileManager.save(dataset_task_6, output)

    # ----------------------------------------
    #   Задача 7.  Найти id автора(ов) курса
    # ----------------------------------------

    dataset_task_7 = dataset_submissions \
        .where("submission_status = 'correct'") \
        .groupBy("user_id_2") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(1) \
        .selectExpr("user_id_2 as author_id")

    output = "output/less_3/task_7"
    Filer.FileManager.save(dataset_task_7, output)

    spark.stop()
