import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, window, sum as spark_sum

spark = SparkSession.builder.appName("EngagementStreaming").getOrCreate()

events_df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/app")
    .option("dbtable", "engagement_events")
    .option("user", "app")
    .option("password", "app")
    .load()
)

content_df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/app")
    .option("dbtable", "content")
    .option("user", "app")
    .option("password", "app")
    .load()
)

enriched = (
    events_df
    .join(content_df, "content_id", "left")
    .withColumn("engagement_seconds", col("duration_ms") / 1000.0)
    .withColumn(
        "engagement_pct",
        expr("""
            CASE
              WHEN duration_ms IS NULL OR length_seconds IS NULL THEN NULL
              ELSE ROUND((duration_ms / 1000.0) / length_seconds, 2)
            END
        """)
    )
)

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

def write_to_redis(df, batch_id):
    for row in df.collect():
        if row.engagement_seconds:
            redis_client.zincrby(
                "top_content_last_10_min",
                row.engagement_seconds,
                row.content_id
            )
    redis_client.expire("top_content_last_10_min", 900)

aggregated = (
    enriched
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        window(col("event_ts"), "10 minutes", "5 seconds"),
        col("content_id")
    )
    .agg(spark_sum("engagement_seconds").alias("total_engagement"))
)

aggregated.writeStream.foreachBatch(write_to_redis).outputMode("update").start()
spark.streams.awaitAnyTermination()
