package com.skdebela.lakehouse.api;

import com.skdebela.lakehouse.api.ObjectStore.StoredObject;
import com.skdebela.lakehouse.ontology.Ontology;
import com.skdebela.lakehouse.ontology.Ontology.ObjectType;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
class ObjectService {

    private static final int MAX_LIMIT = 1000;

    private final ObjectStore store;
    private final Ontology ontology;

    ObjectService(ObjectStore store, Ontology ontology) {
        this.store = store;
        this.ontology = ontology;
    }

    Map<String, Object> get(String typeName, String id) {
        var type = type(typeName);
        return store.findById(type, id)
                .map(o -> toResponse(type, o))
                .orElseThrow(() -> new ApiErrors.NotFound("%s '%s' not found".formatted(typeName, id)));
    }

    List<Map<String, Object>> search(String typeName, Map<String, String> filters, int limit) {
        var type = type(typeName);
        filters.keySet().stream()
                .filter(p -> !type.properties().containsKey(p))
                .findFirst()
                .ifPresent(p -> { throw new ApiErrors.BadRequest("%s has no property '%s'".formatted(typeName, p)); });
        return respond(type, store.findByProperties(type, filters, clamp(limit)));
    }

    List<Map<String, Object>> linked(String typeName, String id, String linkName, int limit) {
        var type = type(typeName);

        var forward = ontology.linkTypes().stream()
                .filter(l -> l.from().equals(typeName) && l.name().equals(linkName)).findFirst();
        if (forward.isPresent()) {
            var target = type(forward.get().to());
            return respond(target, store.followLink(forward.get(), type, target, id));
        }

        var inverse = ontology.linkTypes().stream()
                .filter(l -> l.to().equals(typeName) && l.inverse().equals(linkName)).findFirst()
                .orElseThrow(() -> new ApiErrors.BadRequest("%s has no link '%s'".formatted(typeName, linkName)));
        var source = type(inverse.from());
        return respond(source, store.followInverse(inverse, source, id, clamp(limit)));
    }

    private List<Map<String, Object>> respond(ObjectType type, List<StoredObject> objects) {
        return objects.stream().map(o -> toResponse(type, o)).toList();
    }

    /** The API's shape: $type, the properties, then $links to related objects. */
    private Map<String, Object> toResponse(ObjectType type, StoredObject stored) {
        var response = new LinkedHashMap<String, Object>();
        response.put("$type", type.name());
        response.putAll(stored.properties());

        var id = stored.properties().get("id");
        var links = new LinkedHashMap<String, String>();
        stored.linkTargets().forEach((name, targetId) -> {
            var link = ontology.linksFrom(type.name()).stream().filter(l -> l.name().equals(name)).findFirst().orElseThrow();
            links.put(name, "/objects/%s/%s".formatted(link.to(), targetId));
        });
        ontology.linkTypes().stream()
                .filter(l -> l.to().equals(type.name()))
                .forEach(l -> links.put(l.inverse(), "/objects/%s/%s/links/%s".formatted(type.name(), id, l.inverse())));
        response.put("$links", links);
        return response;
    }

    private ObjectType type(String name) {
        return ontology.type(name)
                .orElseThrow(() -> new ApiErrors.NotFound("Unknown object type '%s'".formatted(name)));
    }

    private static int clamp(int limit) {
        return Math.clamp(limit, 1, MAX_LIMIT);
    }
}