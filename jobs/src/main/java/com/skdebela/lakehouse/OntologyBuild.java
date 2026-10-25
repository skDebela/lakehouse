package com.skdebela.lakehouse;

import com.skdebela.lakehouse.ontology.Ontology;
import com.skdebela.lakehouse.ontology.OntologyLoader;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.spark.sql.functions.col;

public class OntologyBuild {

    public static void main(String[] args) {
        var ontology = OntologyLoader.load();
        var spark = Lakehouse.session("ontology_build");
        var buildId = UUID.randomUUID().toString();

        try {
            // Pin every source to its current snapshot, so all object types come from one consistent moment.
            Map<String, Long> snapshots = new HashMap<>();
            Map<String, Dataset<Row>> sources = new HashMap<>();
            for (var t : ontology.objectTypes()) {
                long snapshot = currentSnapshot(spark, t.source());
                snapshots.put(t.source(), snapshot);
                sources.put(t.name(), spark.sql("SELECT * FROM " + t.source() + " VERSION AS OF " + snapshot).cache());
            }

            var problems = new ArrayList<String>();
            var warnings = new ArrayList<String>();
            checkData(ontology, sources, problems, warnings);
            warnings.forEach(w -> System.out.println("WARNING: " + w));
            if (!problems.isEmpty()) {
                throw new IllegalStateException("Ontology does not match the data:\n  - " + String.join("\n  - ", problems));
            }

            spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ontology");
            for (var t : ontology.objectTypes()) {
                var columns = new ArrayList<Column>();
                t.properties().forEach((property, column) -> columns.add(col(column).alias(property)));
                ontology.linksFrom(t.name())
                        .forEach(l -> columns.add(col(l.foreignKey()).alias(l.linkColumn())));

                var objects = sources.get(t.name()).select(columns.toArray(Column[]::new));

                Map<String, String> props = Map.of(
                        "build_id", buildId,
                        "job", "ontology_build",
                        "code_version", Lakehouse.env("GIT_SHA", "dev"),
                        "ontology_version", String.valueOf(ontology.version()),
                        "input." + t.source(), String.valueOf(snapshots.get(t.source())));

                CommitMetadata.withCommitProperties(props, () -> {
                    objects.writeTo(t.dataset())
                            .using("iceberg")
                            .tableProperty("ontology.type", t.name())
                            .tableProperty("ontology.version", String.valueOf(ontology.version()))
                            .tableProperty("ontology.source", t.source())
                            .tableProperty("ontology.title-property", t.titleProperty())
                            .createOrReplace();
                    return null;
                }, RuntimeException.class);

                System.out.printf("%-10s -> %-28s %,d objects%n", t.name(), t.dataset(), objects.count());
            }
            System.out.println("Ontology v" + ontology.version() + " built (" + buildId + ")");
        } finally {
            spark.stop();
        }
    }

    private static void checkData(Ontology ontology, Map<String, Dataset<Row>> sources,
                                  List<String> problems, List<String> warnings) {
        for (var t : ontology.objectTypes()) {
            var df = sources.get(t.name());
            var columns = Set.of(df.columns());

            t.properties().forEach((property, column) -> {
                if (!columns.contains(column))
                    problems.add("%s.%s: column '%s' not found in %s".formatted(t.name(), property, column, t.source()));
            });
            if (!columns.contains(t.primaryKey())) continue;

            long nullKeys = df.filter(col(t.primaryKey()).isNull()).count();
            long duplicates = df.count() - df.select(t.primaryKey()).distinct().count();
            if (nullKeys > 0) problems.add("%s: %d rows have a null primary key".formatted(t.name(), nullKeys));
            if (duplicates > 0) problems.add("%s: primary key '%s' has %d duplicates".formatted(t.name(), t.primaryKey(), duplicates));
        }

        for (var l : ontology.linkTypes()) {
            var from = sources.get(l.from());
            var to = ontology.type(l.to()).orElseThrow();
            if (!Set.of(from.columns()).contains(l.foreignKey())) {
                problems.add("Link %s: foreign key '%s' not found in %s".formatted(l.name(), l.foreignKey(), l.from()));
                continue;
            }
            // Links pointing at objects that don't exist: reported, but not fatal.
            var fromDf = from.alias("f");
            var toDf = sources.get(l.to()).alias("t");
            long dangling = fromDf
                    .filter(col("f." + l.foreignKey()).isNotNull())
                    .join(toDf, col("f." + l.foreignKey()).equalTo(col("t." + to.primaryKey())), "left_anti")
                    .count();
            if (dangling > 0) warnings.add("Link %s: %d %s objects point to a missing %s".formatted(l.name(), dangling, l.from(), l.to()));
        }
    }

    private static long currentSnapshot(SparkSession spark, String table) {
        return spark.sql("SELECT snapshot_id FROM " + table + ".snapshots ORDER BY committed_at DESC LIMIT 1")
                .first().getLong(0);
    }
}