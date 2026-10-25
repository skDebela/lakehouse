package com.skdebela.lakehouse.ontology;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class OntologyLoader {

    private static final YAMLMapper MAPPER = YAMLMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private OntologyLoader() {
    }

    // Loads from ONTOLOGY_PATH if set, otherwise the ontology.yml bundle in this module.
    public static Ontology load() {
        var path = System.getenv("ONTOLOGY_PATH");
        if (path != null && !path.isBlank()) {
            try (var in = Files.newInputStream(Path.of(path))) {
                return parse(in);
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Cannot read ontology at " + path, e);
            }
        }

        try (var in = OntologyLoader.class.getResourceAsStream("/ontology.yaml")) {
            if (in == null) throw new IllegalStateException("ontology.yaml not found on the classpath");
            return parse(in);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Ontology parse(InputStream in) {
        Ontology ontology = MAPPER.readValue(in, Ontology.class);
        List<String> problems = ontology.problems();
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Invalid ontology: \n -" + String.join("\n - ", problems));
        }

        return ontology;
    }
}
