from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


df = spark.read.load("data/timesData.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
df = df.select("world_rank", "university_name", "country", "total_score", "num_students", "international_students", "female_male_ratio", "year")


df = df.filter(df["total_score"] != "-")

# Divisão da coluna de gêneros

split_col = split(df['female_male_ratio'], ':')
df = df.withColumn('female_ratio', split_col.getItem(0))
df = df.withColumn('male_ratio', split_col.getItem(1))
df.select("university_name","female_male_ratio","female_ratio", "male_ratio").show()

