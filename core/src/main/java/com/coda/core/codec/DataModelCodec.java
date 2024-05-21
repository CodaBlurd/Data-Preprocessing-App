package com.coda.core.codec;

import com.coda.core.entities.DataModel;
import com.coda.core.entities.DataAttributes;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

public final class DataModelCodec<T> implements Codec<DataModel<T>> {

    /**
     * The codec registry.
     * This is used to get the codec for the value.
     */
    private final CodecRegistry codecRegistry;

    /**
     * The data attributes codec.
     * This is used to encode and decode the data attributes.
     */
    private final Codec<DataAttributes> dataAttributesCodec;

    /**
     * DataModelCodec().
     * This constructor is used to create
     * a new instance of the DataModelCodec class.
     * @param registry The codec registry.
     */

    public DataModelCodec(final CodecRegistry registry) {
        this.codecRegistry = registry;
        this.dataAttributesCodec = codecRegistry.get(DataAttributes.class);
    }

    /**
     * encode().
     * This method is used to encode the data model.
     * @param writer The bson writer.
     * @param dataModel The data model.
     * @param encoderContext The encoder context.
     */

    @Override
    public void encode(final BsonWriter writer,
                       final DataModel<T> dataModel,
                       final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString("id", dataModel.getId());

        writer.writeName("attributesMap");
        writer.writeStartDocument();
        for (Map.Entry<String, DataAttributes<T>> entry
                : dataModel.getAttributesMap().entrySet()) {
            writer.writeName(entry.getKey());
            dataAttributesCodec.encode(writer, entry.getValue(),
                    encoderContext);
        }
        writer.writeEndDocument();

        writer.writeEndDocument();
    }

    /**
     * decode().
     * This method is used to decode the data model.
     * @param reader The bson reader.
     * @param decoderContext The decoder context.
     * @return DataModel<T> The data model.
     */

    @Override
    public DataModel<T> decode(final BsonReader reader,
                               final DecoderContext decoderContext) {
        reader.readStartDocument();
        String id = reader.readString("id");

        Map<String, DataAttributes<T>> attributesMap
                = new HashMap<>();
        reader.readName("attributesMap");
        reader.readStartDocument();
        while (reader.readBsonType()
                != org.bson.BsonType.END_OF_DOCUMENT) {
            String key = reader.readName();
            DataAttributes<T> value
                    = dataAttributesCodec.decode(reader, decoderContext);
            attributesMap.put(key, value);
        }
        reader.readEndDocument();

        reader.readEndDocument();
        return new DataModel<>(id, attributesMap);
    }

    /**
     * getEncoderClass().
     * This method is used to get the encoder class.
     * @return Class<DataModel<T>> The data model class.
     */

    @Override
    public Class<DataModel<T>> getEncoderClass() {

        return (
                Class<DataModel<T>>) (Class<?>) DataModel.class;
    }
}
