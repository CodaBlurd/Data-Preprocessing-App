package com.coda.core.deserialization;

import com.coda.core.entities.DataAttributes;
import com.coda.core.exceptions.DataDeserializationException;
import com.coda.core.util.types.ErrorType;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * DataAttributesDeserializer.
 * This class is used to deserialize the data attributes.
 */
public class DataAttributesDeserializer
        extends StdDeserializer<DataAttributes<?>> {

    /**
     * No Argument Constructor().
     */

    public DataAttributesDeserializer() {

        this(null);
    }

    /**
     * constructor().
     * calls superClass constructor
     * with the provided Argument
     * @param vc the class Argument.
     */

    public DataAttributesDeserializer(final Class<?> vc) {

        super(vc);
    }

    /**
     * The  deserialize method deserializes instances of the DataAttributes.
     * it's subClasses where applicable.
     * @param jsonParser the Json parser.
     * @param deserializationContext the context.
     * @return DataAttributes.
     * @throws IOException if an IO error occurs.
     * @throws JacksonException if a JacksonException occurs.
     */

    @Override
    @SuppressWarnings("unchecked cast")
    public DataAttributes<?> deserialize(
            final JsonParser jsonParser,
            final DeserializationContext deserializationContext)
            throws IOException, JacksonException {

        // Get the JSON node from the JSON parser
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // Get the attributes from the JSON node
        String attributeName = node.get("attributeName").asText();
        String type = node.get("type").asText();
        String format = node.get("format").asText();
        boolean required = node.get("required").asBoolean();
        String description = node.get("description").asText();
        String validationRules = node.get("validationRules").asText();

        // Determine the type class
        Class<?> typeClazz;
        try {
            typeClazz = Class.forName(node.get("typeClazz").asText());
        } catch (ClassNotFoundException e) {
            throw new DataDeserializationException(
                    "Error deserializing data attributes: unknown type class",
                    ErrorType.DATA_DESERIALIZATION_ERROR, e);
        }

        // Deserialize value and default value with type safety
        Object value
                = jsonParser.getCodec()
                .treeToValue(node.get("value"), typeClazz);
        Object defaultValue
                = jsonParser.getCodec()
                .treeToValue(node.get("defaultValue"), typeClazz);

        // Get the metadata from the JSON node
        TypeReference<Map<String, Object>> typeRef = new TypeReference<>() { };
        Map<String, Object> metadata
                = jsonParser.getCodec()
                .readValue(node.get("metadata")
                        .traverse(), typeRef);

        // Cast metadata values to the appropriate type
        Map<String, Object> typedMetadata = new HashMap<>();
        for (Map.Entry<String, Object>
                entry : metadata.entrySet()) {
            typedMetadata.put(entry.getKey(),
                    typeClazz.cast(entry.getValue()));
        }

        // Get the last updated date from the JSON node
        Instant lastUpdatedDate
                = jsonParser.getCodec()
                .treeToValue(node.get("lastUpdatedDate"),
                        Instant.class);

        // Construct the DataAttributes object
        DataAttributes<Object> dataAttributes
                = new DataAttributes<>(attributeName, value,
                type, (Class<Object>) typeClazz);

        dataAttributes.setFormat(format);
        dataAttributes.setRequired(required);
        dataAttributes.setDefaultValue(
                typeClazz.cast(defaultValue));
        dataAttributes.setDescription(description);
        dataAttributes.setValidationRules(validationRules);
        dataAttributes.setMetadata(typedMetadata);
        dataAttributes.setLastUpdatedDate(lastUpdatedDate);

        return dataAttributes;
    }
}
