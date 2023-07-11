from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('Erasmus Students Data') \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# CSV -> PySpark DataFrame
df_spark = spark.read \
                .options(header=True, inferSchema=True, delimiter=',') \
                .csv('Erasmus.csv')



def erasmus_data_display():
    df_student_cnt = df_spark.groupBy(["Receiving Country Code", "Sending Country Code"]) \
                             .count()
    df_student_cnt = df_student_cnt.orderBy("Receiving Country Code", "Sending Country Code")

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on every Receiving Country Code")
    df_student_cnt.show(n=df_student_cnt.count())

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on a Receiving Country Code from the following: LV, MK, MT")
    df_student_cnt.where(col("Receiving Country Code").isin(["LV", "MK", "MT"])).show(n=50)





erasmus_data_display()
