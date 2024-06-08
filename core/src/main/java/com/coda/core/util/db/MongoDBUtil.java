package com.coda.core.util.db;

import com.coda.core.codec.DataAttributesCodec;
import com.coda.core.codec.DataModelCodec;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

public final class MongoDBUtil {

    /**
     * Private constructor to prevent instantiation.
     */
    private MongoDBUtil() {
        // Private constructor to prevent instantiation
    }

    /**
     * createMongoClient method.
     * is used to create a MongoClient instance.
     * it's subClasses where applicable.
     * @param url The url of the database.
     * @param username The username of the database.
     * @param password The password of the database.
     * @return MongoClient object.
     */

    public static MongoClient createMongoClient(final String url,
                                                final String username,
                                                final String password) {
        CodecRegistry defaultRegistry
                = MongoClientSettings.getDefaultCodecRegistry();

        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(
                        new DataAttributesCodec<>(defaultRegistry)),
                CodecRegistries.fromCodecs(
                        new DataModelCodec<>(defaultRegistry,
                        new DataAttributesCodec<>(defaultRegistry))),
                defaultRegistry
        );

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(url))
                .credential(MongoCredential.createCredential(username,
                        "admin", password.toCharArray()))
                .codecRegistry(codecRegistry)
                .build();

        return MongoClients.create(settings);
    }
}
