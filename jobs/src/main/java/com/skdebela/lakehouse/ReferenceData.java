package com.skdebela.lakehouse;

public class ReferenceData {

    public static void main(String[] args) {
        var spark = Lakehouse.session("reference_data");

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze");

        spark.sql("""
            CREATE OR REPLACE TABLE lake.bronze.users USING iceberg AS
            SELECT concat('user-', id)                                              AS user_id,
                   concat('User ', id)                                              AS name,
                   element_at(array('ET', 'KE', 'US', 'DE', 'IN'), cast(id % 5 + 1 AS INT)) AS country,
                   date_sub(DATE'2026-01-01', cast(rand(7) * 365 AS INT))           AS signup_date
            FROM range(100)""");

        spark.sql("""
            CREATE OR REPLACE TABLE lake.bronze.products USING iceberg AS
            SELECT concat('product-', id)                                           AS product_id,
                   concat('Product ', id)                                           AS name,
                   element_at(array('books', 'electronics', 'clothing', 'home'), cast(id % 4 + 1 AS INT)) AS category,
                   round(5 + rand(42) * 195, 2)                                     AS price
            FROM range(20)""");

        spark.sql("SELECT count(*) AS users FROM lake.bronze.users").show();
        spark.sql("SELECT * FROM lake.bronze.products ORDER BY product_id").show(20, false);

        spark.stop();
    }
}