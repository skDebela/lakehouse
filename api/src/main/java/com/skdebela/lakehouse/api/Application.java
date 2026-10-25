package com.skdebela.lakehouse.api;

import com.skdebela.lakehouse.ontology.Ontology;
import com.skdebela.lakehouse.ontology.OntologyLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    Ontology ontology() {
        return OntologyLoader.load();
    }

}
