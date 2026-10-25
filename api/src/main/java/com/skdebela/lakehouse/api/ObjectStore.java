package com.skdebela.lakehouse.api;

import com.skdebela.lakehouse.ontology.Ontology.LinkType;
import com.skdebela.lakehouse.ontology.Ontology.ObjectType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Where materialised objects are read from. Knows storage, not API rules. */
interface ObjectStore {

    /** An object as stored: property values by ontology name, and the id each outgoing link points to. */
    record StoredObject(Map<String, Object> properties, Map<String, String> linkTargets) {}

    Optional<StoredObject> findById(ObjectType type, String id);

    /** Equality filters on properties. Callers must pass only properties that exist on the type. */
    List<StoredObject> findByProperties(ObjectType type, Map<String, String> filters, int limit);

    /** Forward link: the target objects that the given source object points to. */
    List<StoredObject> followLink(LinkType link, ObjectType source, ObjectType target, String sourceId);

    /** Inverse link: the source objects that point at the given target id. */
    List<StoredObject> followInverse(LinkType link, ObjectType source, String targetId, int limit);
}