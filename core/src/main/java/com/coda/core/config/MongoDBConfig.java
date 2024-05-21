package com.coda.core.config;

import com.coda.core.codec.DataAttributesCodec;
import com.coda.core.codec.DataModelCodec;
import com.coda.core.util.db.MongoDBConnectionFactory;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecRegistries;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
public class MongoDBConfig {

    /**
     * The MongoDBConnectionFactory object used to connect to the database.
     */
    private final MongoDBConnectionFactory mongoDBConnectionFactory;

    /**
     * MongoDBConfig().
     * This constructor of the MongoDBConfig class.
     * @param connectionFactory The connection factory object.
     */

    public MongoDBConfig(final MongoDBConnectionFactory connectionFactory) {
        this.mongoDBConnectionFactory = connectionFactory;
    }

    /**
     * mongoClient().
     * This bean registers
     * the custom codecs for the
     * DataAttributes and DataModel classes.
     * @return A MongoClient object.
     */

    @Bean
    public static MongoClient mongoClient() {

        CodecRegistry defaultCodecRegistry
                = MongoClientSettings.getDefaultCodecRegistry();
        CodecRegistry customCodecs = CodecRegistries.fromCodecs(
            new DataAttributesCodec<>(defaultCodecRegistry),
            new DataModelCodec<>(defaultCodecRegistry)
        );

        CodecRegistry combinedCodecRegistry
                = CodecRegistries
                .fromRegistries(customCodecs, defaultCodecRegistry);

        MongoClientSettings settings
                = MongoClientSettings.builder()
            .codecRegistry(combinedCodecRegistry)
            .build();

        return MongoClients.create(settings);
    }

    /**
     * MongoTemplate().
     * This method is used to create a connection
     * to the MongoDB database.
     * @return A connection to the database.
     */
    @Bean
    public MongoTemplate mongoTemplate() {
        MongoClient mongoClient = mongoDBConnectionFactory.getMongoClient();
        return new MongoTemplate(mongoClient, "admin");
    }
}
