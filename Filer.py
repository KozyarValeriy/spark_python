import os
import shutil
import pyspark


class FileManager:
    """ Класс для работы с pyspark DataFrame.
        Позволяет удалять каталог перед записью, а также сохранть DataFrame
        в указанное место.
    """

    @classmethod
    def delete(cls, filename: str) -> None:
        """ Метод для удаления каталога с файлами. """
        if os.path.exists(filename):
            shutil.rmtree(filename)

    @classmethod
    def save(cls, data: pyspark.sql.dataframe.DataFrame, filename: str) -> None:
        """ Метод для сохранения DataFrame в файл filename.
            Проверяет, есть ли файл и удаляет его, если находит
        """

        FileManager.delete(filename)
        data \
            .repartition(1) \
            .write \
            .format("csv") \
            .option("header", True) \
            .save(filename)
