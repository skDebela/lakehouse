package com.skdebela.lakehouse;

public class Query {

    public static void main(String[] args) {
        var sql = Lakehouse.env("SQL", "SHOW TABLES IN lake.bronze");
        var spark = Lakehouse.session("query");
        spark.sql(sql).show(50, false);
        spark.stop();
    }
}
