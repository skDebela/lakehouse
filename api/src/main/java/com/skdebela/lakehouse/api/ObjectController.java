package com.skdebela.lakehouse.api;

import com.skdebela.lakehouse.ontology.Ontology;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
class ObjectController {

    private final ObjectService objects;
    private final Ontology ontology;

    ObjectController(ObjectService objects, Ontology ontology) {
        this.objects = objects;
        this.ontology = ontology;
    }

    /** The ontology itself, so clients can discover what exists. */
    @GetMapping("/ontology")
    Ontology ontology() {
        return ontology;
    }

    @GetMapping("/objects/{type}/{id}")
    Map<String, Object> get(@PathVariable String type, @PathVariable String id) {
        return objects.get(type, id);
    }

    @GetMapping("/objects/{type}")
    List<Map<String, Object>> search(@PathVariable String type, @RequestParam Map<String, String> params) {
        var filters = new LinkedHashMap<>(params);
        int limit = parseLimit(filters.remove("limit"));
        return objects.search(type, filters, limit);
    }

    @GetMapping("/objects/{type}/{id}/links/{link}")
    List<Map<String, Object>> linked(@PathVariable String type, @PathVariable String id,
                                     @PathVariable String link,
                                     @RequestParam(defaultValue = "100") int limit) {
        return objects.linked(type, id, link, limit);
    }

    private static int parseLimit(String value) {
        if (value == null) return 100;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ApiErrors.BadRequest("limit must be a number");
        }
    }
}