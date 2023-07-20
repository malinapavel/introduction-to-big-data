from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('Erasmus Students Data') \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

### CSV -> PySpark DataFrame
df_spark = spark.read \
    .options(header=True, inferSchema=True, delimiter=',') \
    .csv('Erasmus.csv')


### Filtering and displaying Erasmus students data
def erasmus_data_filtering():
    df_student_cnt = df_spark.groupBy(["Receiving Country Code", "Sending Country Code"]) \
        .count()
    df_student_cnt = df_student_cnt.orderBy("Receiving Country Code", "Sending Country Code")

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on every Receiving Country Code")
    df_student_cnt.show(n=df_student_cnt.count())

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on a Receiving Country Code from the following: LV, MK, MT")
    df_filtered = df_student_cnt.where(col("Receiving Country Code").isin(["LV", "MK", "MT"]))
    df_filtered.show(n=50)

    return df_filtered


### Storing data into the database
def erasmus_database(df, list_country_codes):
    all_countries_db()
    filtered_data_db(df)
    list_countries_db(list_country_codes)


def all_countries_db():
    df_spark.write \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/erasmus_db") \
        .option("dbtable", "All_Countries") \
        .option("user", "root") \
        .option("password", "dummy") \
        .mode("overwrite") \
        .save()


def filtered_data_db(df):
      df.write \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/erasmus_db") \
        .option("dbtable", "Filtered_Countries") \
        .option("user", "root") \
        .option("password", "dummy") \
        .mode("overwrite") \
        .save()


def list_countries_db(list_country_codes):
    for country in list_country_codes:
        table_name = country + "_Receiving"
        country_df = df_spark.filter(df_spark["Receiving Country Code"] == country).drop("Receiving Country Code")
        country_df.write \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://localhost:3306/erasmus_db") \
            .option("dbtable", table_name) \
            .option("user", "root") \
            .option("password", "dummy") \
            .mode("overwrite") \
            .save()


df_filtered = erasmus_data_filtering()
list_country_codes = ['RO', 'HR', 'IT']
erasmus_database(df_filtered, list_country_codes)
