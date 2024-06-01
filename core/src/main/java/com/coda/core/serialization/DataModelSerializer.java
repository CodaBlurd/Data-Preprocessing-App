package com.coda.core.serialization;

import com.coda.core.entities.DataModel;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * DataAttributesSerializer.
 * This class is used to serialize the data model.
 */

public class DataModelSerializer extends StdSerializer<DataModel<?>> {

    /**
     * DataModelSerializer().
     * This constructor is used to create
     * a new instance of the DataModelSerializer class
     * with null as the argument value.
     */

    public DataModelSerializer() {
        this(null);
    }

    /**
     * DataModelSerializer().
     * This constructor is used to create
     * a new instance of the DataModelSerializer class
     * by passing the class type of the data attributes
     * as the argument value and calling the super class constructor.
     * @param t The class type of the data attributes.
     */

    public DataModelSerializer(final Class<DataModel<?>> t) {
        super(t);
    }

    /**
     * serialize().
     * This method is used to serialize the data model class.
     * @param dataModel The data model object.
     * @param jsonGenerator The json generator.
     * @param serializerProvider The serializer provider.
     * @throws IOException The exception thrown if an error occurs.
     */

    @Override
    public void serialize(final DataModel<?> dataModel,
                          final JsonGenerator jsonGenerator,
                          final SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeObjectId(dataModel.getId());
        jsonGenerator.writeObjectField("attributesMap",
                dataModel.getAttributesMap());

        jsonGenerator.writeEndObject();


    }
}
