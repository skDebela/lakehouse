package com.skdebela.lakehouse.ontology;

import java.util.*;
import java.util.regex.Pattern;

public record Ontology(int version, List<ObjectType> objectTypes, List<LinkType> linkTypes) {

    private static final Pattern TYPE_NAME = Pattern.compile("^[A-Z][A-Za-z0-9]*$");
    private static final Pattern MEMBER_NAME = Pattern.compile("^[a-z][A-Za-z0-9]*$");
    private static final Pattern SOURCE = Pattern.compile("^lake\\.[a-z_]+\\.[a-z_]+$");

    public Optional<ObjectType> type(String name) {
        return objectTypes.stream().filter(t -> t.name().equals(name)).findFirst();
    }

    public List<LinkType> linksFrom(String typeName) {
        return linkTypes.stream().filter(l -> l.from().equals(typeName)).toList();
    }

    /**
     * Structural rules that need no data. Returns every problem found, not just the first.
     */
    public List<String> problems() {
        var problems = new ArrayList<String>();
        var typeNames = new HashSet<String>();

        for (var t : objectTypes) {
            if (!TYPE_NAME.matcher(t.name()).matches())
                problems.add("Type '%s': name must be PascalCase".formatted(t.name()));
            if (!typeNames.add(t.name()))
                problems.add("Type '%s' is defined twice".formatted(t.name()));
            if (!SOURCE.matcher(t.source()).matches())
                problems.add("Type '%s': source '%s' must look like lake.<namespace>.<table>".formatted(t.name(), t.source()));
            if (!t.primaryKey().equals(t.properties().get("id")))
                problems.add("Type '%s': property 'id' must map to the primary key '%s'".formatted(t.name(), t.primaryKey()));
            if (!t.properties().containsKey(t.titleProperty()))
                problems.add("Type '%s': titleProperty '%s' is not a property".formatted(t.name(), t.titleProperty()));
            t.properties().keySet().stream()
                    .filter(p -> !MEMBER_NAME.matcher(p).matches())
                    .forEach(p -> problems.add("Type '%s': property '%s' must be camelCase".formatted(t.name(), p)));

            var seen = new HashSet<String>();
            t.properties().keySet().stream()
                    .filter(p -> !seen.add(p.toLowerCase(java.util.Locale.ROOT)))
                    .forEach(p -> problems.add("Type '%s': property '%s' differs from another only by letter case"
                            .formatted(t.name(), p)));
        }

        for (var l : linkTypes) {
            var from = type(l.from());
            var to = type(l.to());
            if (from.isEmpty()) problems.add("Link '%s': unknown from-type '%s'".formatted(l.name(), l.from()));
            if (to.isEmpty()) problems.add("Link '%s': unknown to-type '%s'".formatted(l.name(), l.to()));
            if (!MEMBER_NAME.matcher(l.name()).matches() || !MEMBER_NAME.matcher(l.inverse()).matches())
                problems.add("Link '%s': name and inverse must be camelCase".formatted(l.name()));
            from.filter(t -> t.properties().containsKey(l.name()))
                    .ifPresent(t -> problems.add("Link '%s' clashes with a property of %s".formatted(l.name(), t.name())));
            to.filter(t -> t.properties().containsKey(l.inverse()))
                    .ifPresent(t -> problems.add("Inverse '%s' clashes with a property of %s".formatted(l.inverse(), t.name())));
        }

        // Each type's outgoing link names and incoming inverse names must be unique together,
        // otherwise "Customer/123/links/orders" would be ambiguous.
        for (var t : objectTypes) {
            var names = new HashSet<String>();
            linkTypes.stream().filter(l -> l.from().equals(t.name())).map(LinkType::name)
                    .filter(n -> !names.add(n))
                    .forEach(n -> problems.add("Type '%s': link name '%s' is used twice".formatted(t.name(), n)));
            linkTypes.stream().filter(l -> l.to().equals(t.name())).map(LinkType::inverse)
                    .filter(n -> !names.add(n))
                    .forEach(n -> problems.add("Type '%s': link name '%s' is used twice".formatted(t.name(), n)));
        }
        return problems;
    }

    public record ObjectType(String name, String source, String primaryKey,
                             String titleProperty, Map<String, String> properties) {

        /**
         * Table the materialised objects live in, e.g. Order -> lake.ontology.orders.
         */
        public String dataset() {
            return "lake.ontology." + name.toLowerCase() + "s";
        }
    }

    public record LinkType(String name, String from, String to, String foreignKey, String inverse) {

        /**
         * Column carrying this link's foreign key in the "from" type's dataset.
         */
        public String linkColumn() {
            return "_link_" + name;
        }
    }
}