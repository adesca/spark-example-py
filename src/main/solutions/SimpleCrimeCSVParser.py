from pyspark.sql import SparkSession
from datetime import datetime, date

import pyspark.sql.functions as F
import pyspark.sql.types as T

if __name__ == "__main__":
    # Create Spark Instance
    spark = SparkSession \
        .builder \
        .appName("SimpleCrimeCSVParser") \
        .getOrCreate()

    # Location of CSV File
    filePath = "/Users/terrencebunkley/dev/git/data-eng-bootcamp/data/crimes/Crimes_-_One_year_prior_to_present.csv"


    def get_DD_MM_YYYY(col):
        return col.split(' ')[0]


    getDate = F.UserDefinedFunction(get_DD_MM_YYYY, T.StringType())

    # Load  DataFrame From the CSV
    df = spark.read.load(filePath, format="csv", sep=",", inferSchema="true", header="true").cache()

    print("The column types are %s" % df.dtypes)
    print("The columns we see in this doc are %s" % df.columns)
    print("The rows count %i" % df.count())
    print("Describe function")
    df.describe().show()

    print("Summary function")
    df.summary().show()

    # Show Arrests
    print("The number of arrests we see in this report are %i" % df.filter(df["ARREST"] == "Y").count())
    df.filter(df["ARREST"] == "Y").show()

    print("The number of NON-arrests we see in this report are %i" % df.filter(df["ARREST"] != "Y").count())

    # Narcotics crimes
    print("The number of Narcotics we see in this report are %i" % df.filter(
        F.col(' PRIMARY DESCRIPTION') == 'NARCOTICS').count())

    # Other offenses
    print("The number of other offenses we see in this report are %i" % df.filter(
        df[" PRIMARY DESCRIPTION"] == 'OTHER OFFENSE').count())
    df.filter(df[" PRIMARY DESCRIPTION"] == 'OTHER OFFENSE').show()

    # Thefts
    print("The number of thefts we see in this report are %i" % df.filter(
        df[" PRIMARY DESCRIPTION"] == 'THEFT').count())

    # Add columns to Dataframe  DATE_AS_STRING
    df = df.withColumn('DATE_AS_STRING', getDate('DATE  OF OCCURRENCE'))
    print(df.take(1))

    # Add Date object to the Dataframe
    df = df.withColumn('Date', F.to_date('DATE_AS_STRING', 'MM/dd/yyyy'))
    print(df.take(1))

    # Filter for crimes that happend in the Spring
    df.filter((df['Date'] > date(2018, 03, 20)) &
              (df['Date'] < date(2018, 06, 21)) &
              (df[' PRIMARY DESCRIPTION'] == 'ARSON')) \
        .show()

    df.filter((df['Date'] > date(2018, 03, 20)) & (df['Date'] < date(2018, 06, 21))) \
        .groupBy(" LOCATION DESCRIPTION").count().sort("count", ascending=False).show(n=100, truncate=False)


    spark.stop()
