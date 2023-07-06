from pyspark.sql import SparkSession


spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName('Erasmus Students Data')\
                    .config("spark.driver.bindAddress", "localhost")\
                    .config("spark.ui.port", "4040")\
                    .getOrCreate()

def test_pyspark():
    # CSV -> PySpark DataFrame -> Pandas DataFrame
    df_spark = spark.read\
                    .options(header=True, inferSchema=True, delimiter=',')\
                    .csv('Erasmus.csv')

    df_spark.show(n=100)



test_pyspark()

