from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

import Filer


def task_1():
    """  Задача 1. Используя данные из файла energy-usage-2010.csv рассчитать суммарное
                   потребление энергии по месяцам (KWH JANUARY 2010, KWH FEBRUARY 2010 и т.д.)
                   в разрезе территорий (COMMUNITY AREA NAME)
    """

    # исходный файл
    input = "input/less_2/energy-usage-2010.csv"

    spark = SparkSession \
        .builder \
        .appName("task 1") \
        .master("local") \
        .getOrCreate()

    # список столбцов, по которым надо найти сумму
    months = ["KWH JANUARY 2010", "KWH FEBRUARY 2010", "KWH MARCH 2010", "KWH APRIL 2010", "KWH MAY 2010",
              "KWH JUNE 2010", "KWH JULY 2010", "KWH AUGUST 2010", "KWH SEPTEMBER 2010", "KWH OCTOBER 2010",
              "KWH NOVEMBER 2010", "KWH DECEMBER 2010"]

    action = dict((x, "sum") for x in months)
    dataset = spark \
        .read \
        .option("header", True) \
        .csv(input)

    for month in months:
        dataset = dataset.withColumn(month, dataset[month].cast(IntegerType()))

    dataset_part1 = dataset \
        .groupBy("COMMUNITY AREA NAME") \
        .agg(action)

    output = "output/less_2/task_1"
    Filer.FileManager.save(dataset_part1, output)


def task_2():
    """  Задача 2. спользуя файл RUvideos.csv найти 10 наиболее популярых видое (по просмотрам) """

    input = "input/less_2/RUvideos.csv"

    spark = SparkSession \
        .builder \
        .appName("task 2") \
        .master("local") \
        .getOrCreate()

    dataset = spark \
        .read \
        .option("header", True) \
        .csv(input)

    dataset = dataset \
        .withColumn("views", dataset["views"].cast(DoubleType()))

    dataset_video_part1 = dataset \
        .orderBy(F.desc("views")) \
        .distinct() \
        .limit(10)

    output = "output/less_2/task_2"
    Filer.FileManager.save(dataset_video_part1, output)

    spark.stop()


def task_3():
    """  Задача 3. Используя файл RUvideos.csv найти наиболее популярные видео (по просмотрам)
                   в разрезе месяцев
    """

    input = "input/less_2/RUvideos.csv"

    spark = SparkSession \
        .builder \
        .appName("task 2") \
        .master("local") \
        .getOrCreate()

    dataset = spark \
        .read \
        .option("header", True) \
        .csv(input)

    # преобразование типов столбцов
    dataset = dataset \
        .withColumn("views", dataset["views"].cast(DoubleType())) \
        .withColumn("publish_time", dataset["publish_time"].cast(DateType()))

    # временный датасет для join-а
    dataset_video_tmp = dataset \
        .select(F.date_format(F.col("publish_time"), "yyyy.MM"), F.col("title"), F.col("views"))

    dataset_video_tmp = dataset_video_tmp \
        .withColumnRenamed("date_format(publish_time, yyyy.MM)", "p_time") \
        .where("p_time is not null")

    # в группе по месяцам ищем видео с максимальным числом просмотров
    dataset_video_part2 = dataset_video_tmp \
        .groupBy("p_time") \
        .max("views") \
        .select(F.expr("p_time as p_time2"), "max(views)")

    # соединяем с временным для определения назания видео с максимальным числом просмотров
    dataset_video_part2 = dataset_video_part2 \
        .join(dataset_video_tmp,
              (dataset_video_part2["max(views)"] == dataset_video_tmp["views"]) &
              (dataset_video_part2["p_time2"] == dataset_video_tmp["p_time"]),
              how="inner") \
        .orderBy(F.desc("max(views)")) \
        .distinct() \
        .selectExpr("p_time as publish_time", "title")

    output = "output/less_2/task_3"
    Filer.FileManager.save(dataset_video_part2, output)

    spark.stop()
