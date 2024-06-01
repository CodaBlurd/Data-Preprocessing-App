package com.coda.core.deserialization;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DataModelDeserializer.
 * This class is used to deserialize the dataModel fields.
 */

public class DataModelDeserializer extends StdDeserializer<DataModel<?>> {

    /**
     * No Argument Constructor().
     */

    public DataModelDeserializer() {
        this(null);
    }

    /**
     * constructor().
     * calls superClass constructor
     * with the provided Argument
     * @param vc the class Argument.
     */

    public DataModelDeserializer(final Class<?> vc) {
        super(vc);

    }

    /**
     * The  deserialize method deserializes instances of the DataAttributes.
     * it's subClasses where applicable.
     * @param jsonParser the Json parser.
     * @param deserializationContext the context.
     * @return DataModel.
     * @throws IOException if an IO error occurs.
     * @throws JacksonException if a JacksonException occurs.
     */

    @Override
    public DataModel<?> deserialize(final JsonParser jsonParser,
                                    final DeserializationContext
                                            deserializationContext)
            throws IOException, JacksonException {

        //read the tree from the getCodec() method
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        // Get the attributes from the tree nodes
        ObjectId id = node.get("id").asText().isEmpty()
                ? null : new ObjectId(node.get("id").asText());
        TypeReference<Map<String, DataAttributes<?>>> typeReference
                = new TypeReference<>() { };

        Map<String, DataAttributes<?>> attributesMap
                = jsonParser.getCodec()
                .readValue(node.get("attributesMap")
                        .traverse(), typeReference);
        Map<String, DataAttributes<Object>> castedAttributesMap
                = castAttributesMap(attributesMap);

        //create a new DataModel instance
        // with the deserialized values.

        return new DataModel<>(id, castedAttributesMap);
    }

    /**
     * castAttributesMap().
     * This method casts the
     * DataAttributes wildcard to a type T
     * @param attributesMap the attributesMap
     * @return the cast attributes map.
     * @param <T> the type.
     */

    @SuppressWarnings("unchecked")
    private <T> Map<String, DataAttributes<T>> castAttributesMap(
            final Map<String, DataAttributes<?>> attributesMap) {
        // Safely cast the map to the required type
        Map<String, DataAttributes<T>> castedMap
                = new HashMap<>();

        for (Map.Entry<String, DataAttributes<?>>
                entry : attributesMap.entrySet()) {
            castedMap.put(entry.getKey(),
                    (DataAttributes<T>) entry.getValue());
        }
        return castedMap;
    }
}
