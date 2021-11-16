from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, struct, collect_list, regexp_replace

spark = SparkSession \
    .builder \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/PMD2021.universities") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.load("data/timesData.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
df = df.select("world_rank", "university_name", "country", "total_score",  
"num_students", "international_students", "female_male_ratio", "year")
df = df.filter(df["total_score"] != "-")

split_col = split(df['female_male_ratio'], ':')
df = df.withColumn('female_ratio', split_col.getItem(0) / 100)
df = df.withColumn('male_ratio', split_col.getItem(1) / 100)
df = df.drop('female_male_ratio')

split_col = split(df['international_students'], '%')
df = df.withColumn('international_students', split_col.getItem(0) / 100)

df = df.withColumn('num_students', regexp_replace('num_students',',', '').cast('int'))

auxDf = df.groupBy(df["university_name"]) \
          .agg({"total_score":"avg"}) \
          .withColumnRenamed("avg(total_score)", "avg_score")

auxDf = auxDf.withColumn("category", \
                                        when(col("avg_score") >= 91, "A") \
                                       .when(col("avg_score") >= 81, "B") \
                                       .when(col("avg_score") >= 71, "C") \
                                       .when(col("avg_score") >= 51, "D") \
                                       .otherwise("E"))

rankingDf = df.withColumn('ranking', struct(col("world_rank").alias('rank'), 
                           col("year"), col("total_score").alias('score')))

rankingDf = rankingDf.groupBy("university_name") \
                     .agg(collect_list('ranking').alias('ranking'))

auxDf = auxDf.withColumnRenamed("university_name", "name")

df = df.join(auxDf, df.university_name == auxDf.name, 'inner') \
.select(df.university_name, df.country, 
df.num_students, df.international_students, df.female_ratio, 
df.male_ratio, auxDf.avg_score, auxDf.category)

rankingDf = rankingDf.withColumnRenamed("university_name", "name")

df = df.join(rankingDf, df.university_name == rankingDf.name, 'inner') \
.select(df.university_name, df.country, df.num_students, 
df.international_students, df.female_ratio, df.male_ratio, df.category, 
df.avg_score, rankingDf.ranking).distinct()

df.write.format("mongo").mode("append").save()