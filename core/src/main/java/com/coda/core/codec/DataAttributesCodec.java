package com.coda.core.codec;

import com.coda.core.entities.DataAttributes;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;


public final class DataAttributesCodec<T> implements Codec<DataAttributes<T>> {

    /**
     * The codec registry.
     * This is used to get the codec for the value.
     */
    private final CodecRegistry codecRegistry;

    /**
     * DataAttributesCodec().
     * This constructor is used to create
     * a new instance of the DataAttributesCodec class.
     * @param registry The codec registry.
     */

    public DataAttributesCodec(final CodecRegistry registry) {
        this.codecRegistry = registry;
    }

    /**
     * decode().
     * This method is used to decode the data attributes.
     * @param bsonReader The bson reader.
     * @param decoderContext The decoder context.
     * @return DataAttributes<T> The data attributes.
     */

    @Override
    public DataAttributes<T> decode(final BsonReader bsonReader,
                                    final DecoderContext decoderContext) {
        String attributeName = null;
        T value = null;
        String type = null;
        Class<T> clazzToken = null;

        bsonReader.readStartDocument();
        while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = bsonReader.readName();
            switch (fieldName) {
                case "attributeName":
                    attributeName = bsonReader.readString();
                    break;
                case "type":
                    type = bsonReader.readString();
                    try {
                        clazzToken = (Class<T>) Class.forName(type);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Class not found: "
                                + type, e);
                    }
                    break;
                case "value":
                    if (clazzToken != null) {
                        Codec<T> codec = codecRegistry.get(clazzToken);
                        value
                                = codec.decode(bsonReader, decoderContext);
                    } else {
                        bsonReader.skipValue();
                    }
                    break;
                default:
                    bsonReader.skipValue();
                    break;
            }
        }
        bsonReader.readEndDocument();
        return new DataAttributes<>(attributeName, value,
                type, clazzToken);
    }

    /**
     * encode().
     * This method is used to encode the data attributes.
     * @param bsonWriter The bson writer.
     * @param dataAttributes The data attributes.
     * @param encoderContext The encoder context.
     */

    @Override
    public void encode(final BsonWriter bsonWriter,
                       final DataAttributes<T> dataAttributes,
                       final EncoderContext encoderContext) {

        bsonWriter.writeStartDocument();

        bsonWriter.writeString("attributeName",
                dataAttributes.getAttributeName());

        bsonWriter.writeString("type",
                dataAttributes.getType());
        bsonWriter.writeName("value");

        if (dataAttributes.getValue() != null) {
            Codec<T> codec = (Codec<T>) codecRegistry
                    .get(dataAttributes.getValue().getClass());

            codec.encode(bsonWriter, dataAttributes.getValue(),
                    encoderContext);
        } else {
            bsonWriter.writeNull();
        }
        bsonWriter.writeEndDocument();
    }

    /**
     * getEncoderClass().
     * This method is used to get the encoder class.
     * @return Class<DataAttributes<T>> The data attributes class.
     */

    @Override
    public Class<DataAttributes<T>> getEncoderClass() {
        return (Class<DataAttributes<T>>) (
                Class<?>) DataAttributes.class;
    }
}
