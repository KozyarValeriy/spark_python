from pyspark import SparkContext
import datetime

import Filer
from lesson_1.MySerializable import LogSerializable


def task_1():
    """ Посчитать кол-во слов в тексте """
    input = "input/less_1/doc1.txt"

    output = "output/less_1/word_count"
    Filer.FileManager.delete(output)

    spark_context = SparkContext("local", "words counter")

    spark_context \
        .textFile(input) \
        .flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x1, x2: x1 + x2) \
        .saveAsTextFile(output)

    spark_context.stop()


def task_2():
    """  Задача с логами. Используется файл logs_example.csv """
    # исходный файл
    input = "input/less_1/logs_example.csv"

    spark_context = SparkContext("local", "words counter")

    # --------------------------------------------------------
    #   Задача 1. Посчитать уникальных пользователей в логах
    # --------------------------------------------------------

    output = "output/less_1/unique_user"
    Filer.FileManager.delete(output)

    spark_context \
        .textFile(input) \
        .map(lambda line: LogSerializable(line.split(","))) \
        .filter(lambda rec: len(rec.login) > 0) \
        .filter(lambda rec: rec.type == "LOGIN") \
        .map(lambda rec: (rec.login, 1)) \
        .reduceByKey(lambda x1, x2: x1 + x2) \
        .saveAsTextFile(output)

    # ------------------------------------------------
    #   Задача 2. Смаппить пользователей и IP адреса
    # ------------------------------------------------

    output = "output/less_1/user_to_ip"
    Filer.FileManager.delete(output)

    spark_context \
        .textFile(input) \
        .map(lambda line: LogSerializable(line.split(","))) \
        .filter(lambda rec: len(rec.login) > 0) \
        .filter(lambda rec: rec.type == "LOGIN") \
        .map(lambda rec: (rec.login, rec.ip)) \
        .reduceByKey(lambda x1, x2: ", ".join([x1, x2]) if x2 not in x1 else x1) \
        .saveAsTextFile(output)

    # ------------------------------------------------
    #   Задача 3. Смаппить IP адреся и пользователей
    # ------------------------------------------------

    output = "output/less_1/ip_to_user"
    Filer.FileManager.delete(output)

    spark_context \
        .textFile(input) \
        .map(lambda line: LogSerializable(line.split(","))) \
        .filter(lambda rec: len(rec.login) > 0) \
        .filter(lambda rec: rec.type == "LOGIN") \
        .map(lambda rec: (rec.ip, rec.login)) \
        .reduceByKey(lambda x1, x2: ", ".join([x1, x2]) if x2 not in x1 else x1) \
        .saveAsTextFile(output)

    # -----------------------------------------------------------------------------------
    #   Задача 4. Найти подозрительные действия аутентификации(LOGIN), т.е. с одного IP
    #             аутентифицировались разные пользователи в интервале секунд
    # -----------------------------------------------------------------------------------

    output = "output/less_1/suspect_user"
    Filer.FileManager.delete(output)

    spark_context \
        .textFile(input) \
        .map(lambda line: LogSerializable(line.split(","))) \
        .filter(lambda rec: len(rec.login) > 0) \
        .filter(lambda rec: rec.type == "LOGIN") \
        .groupBy(lambda rec: rec.ip) \
        .map(lambda data: get_strange(data[0], data[1])) \
        .filter(lambda data: len(data) > 0) \
        .flatMap(lambda data: iter(data)) \
        .saveAsTextFile(output)

    spark_context.stop()


def get_strange(ip: str, users: list) -> list:
    """ Функция для определения странных пользователей.
        Если пользователи заходили с одного ip в течении 2 секунд, то
        считаем, что они подозрительные.
    """

    # время, при котором считаем пользователей подозрительными
    delta = datetime.timedelta(seconds=2)
    # получаем список из итератора
    users = list(users)
    ans = []
    for i in range(len(users) - 1):
        for j in range(i + 1, len(users)):
            if abs(users[i].time - users[j].time) < delta and users[i].login != users[j].login:
                delta_time = abs(users[i].time - users[j].time)
                ans.append(", ".join(
                    [ip, users[i].login, str(users[i].time), users[j].login, str(users[j].time), str(delta_time)]))
    return ans
