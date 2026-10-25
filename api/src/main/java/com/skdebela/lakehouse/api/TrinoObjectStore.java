package com.skdebela.lakehouse.api;

import com.skdebela.lakehouse.ontology.Ontology;
import com.skdebela.lakehouse.ontology.Ontology.LinkType;
import com.skdebela.lakehouse.ontology.Ontology.ObjectType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Repository
class TrinoObjectStore implements ObjectStore {

    // TODO: Index objects into a fast store built for lookups. After each OntologyBuild,
    //  copy the objects into Postgres (or OpenSearch for search),  and have the API read from there

    private final JdbcClient jdbc;
    private final Ontology ontology;

    TrinoObjectStore(JdbcClient jdbc, Ontology ontology) {
        this.jdbc = jdbc;
        this.ontology = ontology;
    }

    private static Object normalize(Object value) {
        return switch (value) {
            case java.sql.Timestamp ts -> ts.toInstant();
            case java.sql.Date d -> d.toLocalDate();
            case null, default -> value;
        };
    }

    private static String lower(String s) {
        return s.toLowerCase(Locale.ROOT);
    }

    @Override
    public Optional<StoredObject> findById(ObjectType type, String id) {
        return query(type, "SELECT * FROM " + type.dataset() + " WHERE id = ?", List.of(id))
                .stream().findFirst();
    }

    @Override
    public List<StoredObject> findByProperties(ObjectType type, Map<String, String> filters, int limit) {
        var sql = new StringBuilder("SELECT * FROM " + type.dataset() + " WHERE true");
        var args = new ArrayList<Object>();
        filters.forEach((property, value) -> {
            // Property names were validated against the ontology by the caller; values are always parameters.
            sql.append(" AND CAST(").append(property).append(" AS varchar) = ?");
            args.add(value);
        });
        sql.append(" ORDER BY id LIMIT ").append(limit);
        return query(type, sql.toString(), args);
    }

    @Override
    public List<StoredObject> followLink(LinkType link, ObjectType source, ObjectType target, String sourceId) {
        var sql = "SELECT t.* FROM %s t JOIN %s s ON t.id = s.%s WHERE s.id = ?"
                .formatted(target.dataset(), source.dataset(), link.linkColumn());
        return query(target, sql, List.of(sourceId));
    }

    @Override
    public List<StoredObject> followInverse(LinkType link, ObjectType source, String targetId, int limit) {
        var sql = "SELECT * FROM %s WHERE %s = ? ORDER BY id LIMIT %d"
                .formatted(source.dataset(), link.linkColumn(), limit);
        return query(source, sql, List.of(targetId));
    }

    private List<StoredObject> query(ObjectType type, String sql, List<?> args) {
        return jdbc.sql(sql).params(args).query().listOfRows().stream()
                .map(row -> toStored(type, row))
                .toList();
    }

    /**
     * Trino reports column names in lower case; map them back to ontology names here, and only here.
     */
    private StoredObject toStored(ObjectType type, Map<String, Object> row) {
        var properties = new LinkedHashMap<String, Object>();
        type.properties().keySet().forEach(p -> properties.put(p, normalize(row.get(lower(p)))));

        var linkTargets = new LinkedHashMap<String, String>();
        ontology.linksFrom(type.name()).forEach(link -> {
            var target = row.get(lower(link.linkColumn()));
            if (target != null) linkTargets.put(link.name(), target.toString());
        });
        return new StoredObject(properties, linkTargets);
    }
}