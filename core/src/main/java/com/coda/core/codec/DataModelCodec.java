package com.coda.core.codec;

import com.coda.core.entities.DataModel;
import com.coda.core.entities.DataAttributes;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.Map;

public final class DataModelCodec<T> implements Codec<DataModel<T>> {

    /**
     * The CodecRegistry used to encode and decode the DataModel.
     */

    private final CodecRegistry codecRegistry;

    /**
     * The Codec used to encode
     * and decode the DataAttributes.
     */
    private final Codec<DataAttributes<T>> dataAttributesCodec;

    /**
     * Constructor().
     * @param registry CodecRegistry used to
     *                encode and decode the DataModel.
     * @param codec Codec used to encode
     *                            and decode the DataAttributes.
     */

    public DataModelCodec(final CodecRegistry registry,
                          final Codec<DataAttributes<T>> codec) {
        this.codecRegistry = registry;
        this.dataAttributesCodec = codec;
    }

    /**
     * Encode the DataModel.
     * @param writer BsonWriter used to encode the DataModel.
     * @param dataModel DataModel to encode.
     * @param encoderContext context used to encode the DataModel.
     */

    @Override
    public void encode(final BsonWriter writer,
                       final DataModel<T> dataModel,
                       final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeObjectId("id", dataModel.getId());

        writer.writeName("attributesMap");
        writer.writeStartDocument();
        for (Map.Entry<String, DataAttributes<T>>
                entry : dataModel.getAttributesMap().entrySet()) {

            if (entry.getKey() != null && entry.getValue() != null) {
                writer.writeName(entry.getKey());
                dataAttributesCodec.encode(writer,
                        entry.getValue(), encoderContext);
            }
        }
        writer.writeEndDocument();

        writer.writeEndDocument();
    }

    /**
     * Decode the DataModel.
     * @param reader BsonReader used to decode the DataModel.
     * @param decoderContext context used to decode the DataModel.
     * @return DataModel decoded.
     */

    @Override
    public DataModel<T> decode(final BsonReader reader,
                               final DecoderContext decoderContext) {
        reader.readStartDocument();
        ObjectId id = reader.readObjectId("id");

        Map<String, DataAttributes<T>> attributesMap = new HashMap<>();
        reader.readName("attributesMap");
        reader.readStartDocument();
        while (reader.readBsonType() != org.bson.BsonType.END_OF_DOCUMENT) {
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
     * Get the Class of the DataModel.
     * @return Class of the DataModel.
     */

    @Override
    public Class<DataModel<T>> getEncoderClass() {
        return (Class<DataModel<T>>) (Class<?>) DataModel.class;
    }
}
