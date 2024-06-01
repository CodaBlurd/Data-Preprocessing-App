package com.coda.core.serialization;

import com.coda.core.entities.DataAttributes;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * DataAttributesSerializer.
 * This class is used to serialize the data attributes.
 */

public class DataAttributesSerializer extends StdSerializer<DataAttributes<?>> {

    /**
     * DataAttributesSerializer().
     * This constructor is used to create
     * a new instance of the DataAttributesSerializer class
     * with null as the argument value.
     */

    public DataAttributesSerializer() {
        this(null);
    }

    /**
     * DataAttributesSerializer().
     * This constructor is used to create
     * a new instance of the DataAttributesSerializer class
     * by passing the class type of the data attributes
     * as the argument value and calling the super class constructor.
     * @param t The class type of the data attributes.
     */

    public DataAttributesSerializer(final Class<DataAttributes<?>> t) {
        super(t);
    }

    /**
     * serialize().
     * This method is used to serialize the data attributes.
     * @param dataAttributes The data attributes.
     * @param jsonGenerator The json generator.
     * @param serializerProvider The serializer provider.
     * @throws IOException The exception thrown if an error occurs.
     */
    @Override
    public void serialize(final DataAttributes<?> dataAttributes,
                          final JsonGenerator jsonGenerator,
                          final SerializerProvider serializerProvider)
            throws IOException {

        jsonGenerator.writeStartObject();

jsonGenerator.writeStringField("attributeName",
                dataAttributes.getAttributeName());
jsonGenerator.writeObjectField("value",
        dataAttributes.getValue());
jsonGenerator.writeStringField("type",
        dataAttributes.getType());
jsonGenerator.writeStringField("format",
        dataAttributes.getFormat());
jsonGenerator.writeBooleanField("required",
        dataAttributes.isRequired());
jsonGenerator.writeObjectField("defaultValue",
        dataAttributes.getDefaultValue());
jsonGenerator.writeStringField("description",
        dataAttributes.getDescription());
jsonGenerator.writeStringField("validationRules",
        dataAttributes.getValidationRules());
jsonGenerator.writeObjectField("metadata",
        dataAttributes.getMetadata());
jsonGenerator.writeObjectField("lastUpdatedDate",
        dataAttributes.getLastUpdatedDate());
jsonGenerator.writeStringField("typeClazz",
        dataAttributes.getTypeClazzName());

jsonGenerator.writeEndObject();

    }
}
