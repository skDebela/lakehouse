package com.skdebela.lakehouse;

import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class BronzeIngest {

    private static final StructType EVENT_SCHEMA = new StructType()
            .add("eventId", DataTypes.StringType)
            .add("userId", DataTypes.StringType)
            .add("page", DataTypes.StringType)
            .add("type", DataTypes.StringType)
            .add("productId", DataTypes.StringType)
            .add("timestamp", DataTypes.LongType);

    public static void main(String[] args) throws Exception {
        var spark = Lakehouse.session("bronze_ingest");

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze");
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.bronze.clickstream_events (
                event_id        STRING,
                user_id         STRING,
                page            STRING,
                type            STRING,
                product_id      STRING,
                event_time      TIMESTAMP,
                kafka_partition INT,
                kafka_offset    BIGINT,
                kafka_timestamp TIMESTAMP,
                raw             STRING,
                ingested_at     TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (days(event_time))""");

        var events = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", Lakehouse.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .option("subscribe", "clickstream")
                .option("startingOffsets", "earliest")
                .load()
                .select(
                        col("value").cast("string").alias("raw"),
                        col("partition").alias("kafka_partition"),
                        col("offset").alias("kafka_offset"),
                        col("timestamp").alias("kafka_timestamp"))
                .withColumn("e", from_json(col("raw"), EVENT_SCHEMA))
                .select(
                        col("e.eventId").alias("event_id"),
                        col("e.userId").alias("user_id"),
                        col("e.page").alias("page"),
                        col("e.type").alias("type"),
                        col("e.productId").alias("product_id"),
                        expr("timestamp_millis(e.timestamp)").alias("event_time"),
                        col("kafka_partition"),
                        col("kafka_offset"),
                        col("kafka_timestamp"),
                        col("raw"),
                        current_timestamp().alias("ingested_at"));

        var query = events.writeStream()
                .queryName("bronze_clickstream")
                .format("iceberg")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("1 minute"))
                .option("fanout-enabled", "true")
                .option("checkpointLocation",
                        Lakehouse.env("CHECKPOINT_DIR", "/tmp/checkpoints/lakehouse/bronze-events"))
                .toTable("lake.bronze.clickstream_events");

        System.out.println("Ingesting into lake.bronze.clickstream_events. Spark UI: http://localhost:4040");
        query.awaitTermination();
    }
}
