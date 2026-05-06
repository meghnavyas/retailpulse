from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

INPUT_PATH = "include/data/raw/customers.csv"
OUTPUT_PATH = "include/data/processed/customers"


def define_schema():
    """Define and enforce schema for customers data"""
    return StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("signup_date", DateType(), nullable=True),
        StructField("acquisition_channel", StringType(), nullable=True),
        StructField("age_group", StringType(), nullable=True),
        StructField("is_premium", BooleanType(), nullable=True)
    ])


def create_spark_session():
    # Create a SparkSession with:
    # - app name: "retailpulse_transform_customers"
    # - master: use all available local cores
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("retailpulse_transform_customers") \
        .getOrCreate()

    return spark


def transform(df):
    # 1. Remove duplicate customers
    #    Hint: which single column uniquely identifies a customer?
    df = df.dropDuplicates(["customer_id"])

    # 2. Normalise age_group column
    #    Hint: two operations — remove surrounding whitespace
    df = df.withColumn("age_group", F.trim(F.col("age_group")))
 
    # 3. Cast is_premium to integer
    #    Hint: Snowflake handles booleans inconsistently — 0/1 is safer
    df = df.withColumn("is_premium", F.col("is_premium").cast("integer"))

    # 4. Add processed_at column — current timestamp
    df = df.withColumn("processed_at", F.current_timestamp())
    
    # 5. Add source column — string literal "generate_data_v1"
    df = df.withColumn("source", F.lit("generate_data_v1"))

    return df


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Define schema and enforce it when reading CSV
    schema = define_schema()
    df = spark.read.csv(INPUT_PATH, header=True, schema=schema)

    print(f"Raw count: {df.count()}")

    df = transform(df)

    print(f"Processed count: {df.count()}")
    df.printSchema()

    # Write as parquet, overwrite if path already exists
    df.write.parquet(OUTPUT_PATH, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
