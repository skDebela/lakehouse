package com.skdebela.lakehouse;

import org.apache.iceberg.spark.CommitMetadata;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.UUID;

public class SilverBuild {

    private static final String JOB = "silver_build";
    private static final String BRONZE = "lake.bronze.clickstream_events";

    public static void main(String[] args) {
        var spark = Lakehouse.session(JOB);
        createTables(spark);

        var buildId = UUID.randomUUID().toString();
        var codeVersion = Lakehouse.env("GIT_SHA", "dev");
        var startedAt = System.currentTimeMillis();

        Long pinned = currentSnapshot(spark);
        Long previous = lastProcessedSnapshot(spark);
        if (pinned == null) {
            System.out.println("Bronze is empty, nothing to build.");
            return;
        }
        if (pinned.equals(previous)) {
            System.out.println("No new bronze data since snapshot " + previous + ".");
            return;
        }

        try {
            // 1. Read only what's new: the whole table on the first build, then just the appends since last time.
            var reader = spark.read().format("iceberg");
            var increment = previous == null
                    ? spark.sql("SELECT * FROM " + BRONZE + " VERSION AS OF " + pinned)
                    : spark.read().format("iceberg")
                    .option("start-snapshot-id", previous)
                    .option("end-snapshot-id", pinned)
                    .load(BRONZE);
            increment.createOrReplaceTempView("bronze_increment");

            // 2. Classify rows: good ones get reject_reason = NULL.
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW classified AS
                SELECT *,
                       CASE WHEN event_id IS NULL   THEN 'unparseable_or_missing_event_id'
                            WHEN user_id IS NULL    THEN 'missing_user_id'
                            WHEN event_time IS NULL THEN 'missing_event_time'
                            WHEN type IS NULL OR type NOT IN ('page_view', 'click', 'purchase') THEN 'unknown_type'
                       END AS reject_reason
                FROM bronze_increment""");

            // 3. De-duplicate within this increment (earliest Kafka offset wins). Cached: used several times below.
            spark.sql("""
                CACHE TABLE new_events AS
                SELECT event_id, user_id, page, type, product_id, event_time
                FROM (
                    SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY kafka_partition, kafka_offset) AS rn
                    FROM classified WHERE reject_reason IS NULL
                ) WHERE rn = 1""");

            long processed = spark.table("new_events").count();
            long rejected = spark.sql("SELECT * FROM classified WHERE reject_reason IS NOT NULL").count();

            // Every write below is stamped with this build's provenance in its Iceberg snapshot.
            Map<String, String> props = Map.of(
                    "build_id", buildId,
                    "job", JOB,
                    "code_version", codeVersion,
                    "input." + BRONZE, String.valueOf(pinned));

            // 4. Events: duplicates across builds are skipped by the MERGE.
            commit(spark, props, """
                MERGE INTO lake.silver.events t
                USING new_events s
                ON t.event_id = s.event_id AND t.event_time = s.event_time
                WHEN NOT MATCHED THEN INSERT *""");

            // 5. Orders: purchases joined with product and user reference data.
            commit(spark, props, """
                MERGE INTO lake.silver.orders t
                USING (
                    SELECT e.event_id   AS order_id,
                           e.user_id    AS customer_id,
                           e.product_id,
                           p.category,
                           p.price      AS amount,
                           u.country,
                           e.event_time AS order_time
                    FROM new_events e
                    JOIN lake.bronze.products p ON e.product_id = p.product_id
                    LEFT JOIN lake.bronze.users u ON e.user_id = u.user_id
                    WHERE e.type = 'purchase'
                ) s
                ON t.order_id = s.order_id AND t.order_time = s.order_time
                WHEN NOT MATCHED THEN INSERT *""");

            // 6. Rejected rows are kept, never silently dropped.
            if (rejected > 0) {
                commit(spark, props, """
                    INSERT INTO lake.silver.rejected_events
                    SELECT raw, kafka_partition, kafka_offset, reject_reason, '%s', current_timestamp()
                    FROM classified WHERE reject_reason IS NOT NULL""".formatted(buildId));
            }

            // 7. Customers: small, so rebuilt in full every time.
            commit(spark, props, """
                INSERT OVERWRITE lake.silver.customers
                WITH activity AS (
                    SELECT user_id, min(event_time) AS first_seen, max(event_time) AS last_seen
                    FROM lake.silver.events GROUP BY user_id
                ),
                spend AS (
                    SELECT customer_id, count(*) AS total_orders, sum(amount) AS lifetime_value
                    FROM lake.silver.orders GROUP BY customer_id
                )
                SELECT u.user_id, u.name, u.country, u.signup_date,
                       a.first_seen, a.last_seen,
                       coalesce(s.total_orders, 0), coalesce(s.lifetime_value, 0)
                FROM lake.bronze.users u
                LEFT JOIN activity a ON a.user_id = u.user_id
                LEFT JOIN spend s ON s.customer_id = u.user_id""");

            recordBuild(spark, buildId, codeVersion, pinned, previous, startedAt, "succeeded", processed, rejected);
            System.out.printf("Build %s succeeded: bronze %s -> %d, %d new events, %d rejected%n",
                    buildId, previous, pinned, processed, rejected);
        } catch (RuntimeException e) {
            recordBuild(spark, buildId, codeVersion, pinned, previous, startedAt, "failed", 0, 0);
            throw e;
        } finally {
            spark.stop();
        }
    }

    private static void commit(SparkSession spark, Map<String, String> props, String sql) {
        CommitMetadata.withCommitProperties(props, () -> spark.sql(sql), RuntimeException.class);
    }

    private static Long currentSnapshot(SparkSession spark) {
        var rows = spark.sql("SELECT snapshot_id FROM " + BRONZE + ".snapshots ORDER BY committed_at DESC LIMIT 1")
                .collectAsList();
        return rows.isEmpty() ? null : rows.getFirst().getLong(0);
    }

    private static Long lastProcessedSnapshot(SparkSession spark) {
        var rows = spark.sql("""
            SELECT input_snapshot FROM lake.meta.builds
            WHERE job = '%s' AND status = 'succeeded'
            ORDER BY finished_at DESC LIMIT 1""".formatted(JOB)).collectAsList();
        return rows.isEmpty() ? null : rows.getFirst().getLong(0);
    }

    private static void recordBuild(SparkSession spark, String buildId, String codeVersion, long input,
                                    Long previous, long startedAt, String status, long processed, long rejected) {
        spark.sql("""
            INSERT INTO lake.meta.builds VALUES
            ('%s', '%s', '%s', %d, %s, timestamp_millis(%d), current_timestamp(), '%s', %d, %d)"""
                .formatted(buildId, JOB, codeVersion, input, previous == null ? "NULL" : previous,
                        startedAt, status, processed, rejected));
    }

    private static void createTables(SparkSession spark) {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.meta");

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.events (
                event_id STRING, user_id STRING, page STRING, type STRING,
                product_id STRING, event_time TIMESTAMP
            ) USING iceberg PARTITIONED BY (days(event_time))""");

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.orders (
                order_id STRING, customer_id STRING, product_id STRING, category STRING,
                amount DOUBLE, country STRING, order_time TIMESTAMP
            ) USING iceberg PARTITIONED BY (days(order_time))""");

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.customers (
                customer_id STRING, name STRING, country STRING, signup_date DATE,
                first_seen TIMESTAMP, last_seen TIMESTAMP,
                total_orders BIGINT, lifetime_value DOUBLE
            ) USING iceberg""");

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.rejected_events (
                raw STRING, kafka_partition INT, kafka_offset BIGINT,
                reason STRING, build_id STRING, rejected_at TIMESTAMP
            ) USING iceberg""");

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.meta.builds (
                build_id STRING, job STRING, code_version STRING,
                input_snapshot BIGINT, previous_snapshot BIGINT,
                started_at TIMESTAMP, finished_at TIMESTAMP,
                status STRING, rows_processed BIGINT, rows_rejected BIGINT
            ) USING iceberg""");
    }
}