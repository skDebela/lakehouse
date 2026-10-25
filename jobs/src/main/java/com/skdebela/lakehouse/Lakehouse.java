package com.skdebela.lakehouse;

import org.apache.spark.sql.SparkSession;

public final class Lakehouse {

    private Lakehouse() {}

    public static SparkSession session(String appName) {
        var builder = SparkSession.builder()
                .appName(appName)
                .master(env("SPARK_MASTER", "local[*]"))
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.lake.type", "rest")
                .config("spark.sql.catalog.lake.uri", env("CATALOG_URI", "http://localhost:8181"))
                .config("spark.sql.catalog.lake.warehouse", "s3://warehouse/")
                .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.lake.s3.endpoint", env("S3_ENDPOINT", "http://localhost:9002"))
                .config("spark.sql.catalog.lake.s3.path-style-access", "true")
                .config("spark.sql.catalog.lake.s3.access-key-id", env("S3_ACCESS_KEY", "admin"))
                .config("spark.sql.catalog.lake.s3.secret-access-key", env("S3_SECRET_KEY", "password"))
                .config("spark.sql.catalog.lake.client.region", "us-east-1");

        // Lineage is on by default; Set OPENLINEAGE_ENABLED=false to run without Marquez.
        if (!"false".equalsIgnoreCase(env("OPENLINEAGE_ENABLED", "true"))) {
            builder.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
                    .config("spark.openlineage.transport.type", "http")
                    .config("spark.openlineage.transport.url", env("OPENLINEAGE_URL", "http://localhost:5002"))
                    .config("spark.openlineage.namespace", env("OPENLINEAGE_NAMESPACE", "lakehouse"));
        }
        return builder.getOrCreate();
    }

    static String env(String key, String fallback) {
        var value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}