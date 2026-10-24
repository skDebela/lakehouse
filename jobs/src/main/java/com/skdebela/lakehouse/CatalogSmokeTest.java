package com.skdebela.lakehouse;

public class CatalogSmokeTest {

    public static void main(String[] args) {
        var spark = Lakehouse.session("catalog-smoke-test");

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze");
        spark.sql("DROP TABLE IF EXISTS lake.bronze.smoke_test PURGE");
        spark.sql("CREATE TABLE lake.bronze.smoke_test (id INT, name STRING) USING iceberg");

        spark.sql("INSERT INTO lake.bronze.smoke_test VALUES (1, 'first')");
        spark.sql("INSERT INTO lake.bronze.smoke_test VALUES (2, 'second')");

        System.out.println("== Snapshots ==");
        spark.sql("""
            SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
            FROM lake.bronze.smoke_test.snapshots
            ORDER BY committed_at""").show(false);

        long firstSnapshot = spark.sql("""
            SELECT snapshot_id FROM lake.bronze.smoke_test.snapshots
            ORDER BY committed_at LIMIT 1""").first().getLong(0);

        System.out.println("== Current table ==");
        spark.sql("SELECT * FROM lake.bronze.smoke_test ORDER BY id").show();

        System.out.println("== Time travel to snapshot " + firstSnapshot + " ==");
        spark.sql("SELECT * FROM lake.bronze.smoke_test VERSION AS OF " + firstSnapshot).show();

        spark.stop();
    }
}