package com.coda.core.codec;

import com.coda.core.entities.DataAttributes;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

public class DataAttributesCodec<T> implements Codec<DataAttributes<T>> {

    /**
     * The CodecRegistry.
     * used to encode and decode the DataAttributes.
     */

    private final CodecRegistry codecRegistry;

    /**
     * Constructor().
     * @param registry the codec registry used
     *                      to encode and decode the DataAttributes.
     */

    public DataAttributesCodec(final CodecRegistry registry) {
        this.codecRegistry = registry;
    }

    /**
     * Encode the DataAttributes.
     * @param writer BsonWriter used to encode the DataAttributes.
     * @param dataAttributes DataAttributes to encode.
     * @param encoderContext context used to encode the DataAttributes.
     */

    @Override
    public final void encode(final BsonWriter writer,
                       final DataAttributes<T> dataAttributes,
                       final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString("attributeName", dataAttributes.getAttributeName());
        writer.writeString("type", dataAttributes.getType());
        writer.writeString("format", dataAttributes.getFormat());
        writer.writeBoolean("required", dataAttributes.isRequired());
        writer.writeString("description", dataAttributes.getDescription());
        writer.writeString("validationRules",
                dataAttributes.getValidationRules());

        Document metaData = new Document();
        for (Map.Entry<String, T>
                entry : dataAttributes.getMetadata().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                metaData.put(entry.getKey(), entry.getValue());
            }
        }
        writer.writeName("metadata");
        DocumentCodec documentCodec
                = new DocumentCodec(codecRegistry);
        documentCodec.encode(writer, metaData, encoderContext);

        writer.writeString("typeClazzName",
                dataAttributes.getTypeClazzName());
        writer.writeEndDocument();
    }

    @Override
    public final DataAttributes<T> decode(final BsonReader reader,
                                          final DecoderContext decoderContext) {
        reader.readStartDocument();
        String attributeName = reader.readString("attributeName");
        String type = reader.readString("type");
        String format = reader.readString("format");
        boolean required = reader.readBoolean("required");
        String description = reader.readString("description");
        String validationRules = reader.readString("validationRules");

        reader.readName("metadata");
        DocumentCodec documentCodec = new DocumentCodec(codecRegistry);
        Document metaData = documentCodec.decode(reader, decoderContext);
        Map<String, T> metadata = new HashMap<>();
        for (Map.Entry<String, Object> entry : metaData.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                metadata.put(entry.getKey(), (T) entry.getValue());
            }
        }
        String typeClazzName = reader.readString("typeClazzName");
        Class<T> typeClazz;
        try {
            typeClazz = (Class<T>) Class.forName(typeClazzName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found: " + typeClazzName, e);
        }

        reader.readEndDocument();
        return new DataAttributes<>(attributeName, metadata, type, typeClazz);
    }

    @Override
    public final Class<DataAttributes<T>> getEncoderClass() {
        return (Class<DataAttributes<T>>) (Class<?>) DataAttributes.class;
    }
}
