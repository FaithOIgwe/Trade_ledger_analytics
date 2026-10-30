"""
Copy analytics.fct_trades from Postgres into two Iceberg tables:

  lake.brokerage.trades            partitioned by day of executed_at
  lake.brokerage.trades_unpartitioned  same data, no partitioning

Having both lets you run the same query in Trino and compare the plans.

Run inside the spark container:
  docker compose exec spark spark-submit \
      --packages org.postgresql:postgresql:42.7.4 \
      /home/iceberg/jobs/trades_to_iceberg.py
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("trades_to_iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "rest")
    .config("spark.sql.catalog.lake.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lake.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.lake.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.lake.s3.path-style-access", "true")
    .getOrCreate()
)

trades = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/brokerage")
    .option("dbtable", "analytics.fct_trades")
    .option("user", "brokerage")
    .option("password", "brokerage")
    .option("driver", "org.postgresql.Driver")
    .load()   # uuid columns arrive as strings over JDBC, which is what we want in Iceberg
)
trades.createOrReplaceTempView("src_trades")

spark.sql("create namespace if not exists lake.brokerage")

spark.sql("""
    create or replace table lake.brokerage.trades
    using iceberg
    partitioned by (days(executed_at))
    as select * from src_trades
""")

spark.sql("""
    create or replace table lake.brokerage.trades_unpartitioned
    using iceberg
    as select * from src_trades
""")

for t in ["trades", "trades_unpartitioned"]:
    files = spark.sql(f"select count(*) from lake.brokerage.{t}.files").first()[0]
    rows = spark.sql(f"select count(*) from lake.brokerage.{t}").first()[0]
    print(f"{t}: {rows} rows in {files} data files")

spark.stop()
