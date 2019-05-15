from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt


def rename_columns(crimes):
    for x in range(17):
        if x == 0:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'CASE#')
        elif x == 1:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'DATE_OF_OCCURRENCE')
        elif x == 2:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'BLOCK')
        elif x == 3:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'IUCR')
        elif x == 4:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'PRIMARY_DESCRIPTION')
        elif x == 5:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'SECONDARY_DESCRIPTION')
        elif x == 6:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'LOCATION_DESCRIPTION')
        elif x == 7:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'ARREST')
        elif x == 8:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'DOMESTIC')
        elif x == 9:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'BEAT')
        elif x == 10:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'WARD')
        elif x == 11:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'FBI_CD')
        elif x == 12:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'X_COORDINATE')
        elif x == 13:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'Y_COORDINATE')
        elif x == 14:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'LATITUDE')
        elif x == 15:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'LONGITUDE')
        elif x == 16:
            crimes = crimes.withColumnRenamed('_c' + str(x), 'LOCATION')
        else:
            print('Column not found...')
    return crimes


spark = SparkSession.builder.appName("Crimes").getOrCreate()
crimes = spark.read.option("inferSchema", "true").csv("./data/crimes/Crimes.csv")
crimes_df = rename_columns(crimes)
crimes_df.printSchema()
count = [item[1] for item in crimes_df.groupBy("PRIMARY_DESCRIPTION").count().collect()]
description = [item[0] for item in crimes_df.groupBy("PRIMARY_DESCRIPTION").count().collect()]
number_of_crimes_per_year_type = {"description": description, "count": count}
number_of_crimes_per_year_type = pd.DataFrame(number_of_crimes_per_year_type)

number_of_crimes_per_year_type = number_of_crimes_per_year_type.sort_values(by="description")
number_of_crimes_per_year_type.plot(figsize=(18, 10), kind="barh", color="red", x="description", y="count",
                                    legend=False)

plt.xlabel("Number of Crimes", fontsize=12)
plt.ylabel("Type of Crimes", fontsize=12)
plt.title("Number of Crimes Per Year", fontsize=28)
plt.xticks(size=18)
plt.yticks(size=6)

plt.show()
