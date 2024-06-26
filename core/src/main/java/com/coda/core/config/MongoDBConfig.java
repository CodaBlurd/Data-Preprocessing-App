package com.coda.core.config;

import com.coda.core.util.db.MongoDBUtil;
import com.mongodb.client.MongoClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

@Configuration
public class MongoDBConfig {

    /**
     * MongoDB's properties bean.
     */
    private final MongoDBProperties mongoDBProperties;

    /**
     * MongoDBConfig constructor.
     * @param properties MongoDB's properties bean.
     */
    @Autowired
    public MongoDBConfig(final MongoDBProperties properties) {
        this.mongoDBProperties = properties;
    }

    /**
     * MongoDB client bean.
     * this bean is used to create a MongoClient instance.
     * @return MongoClient object.
     */

    @Bean
    @Primary
    public MongoClient mongoClient() {

        return MongoDBUtil
                .createMongoClient(
                        mongoDBProperties.getUrl(),
                        mongoDBProperties.getUsername(),
                        mongoDBProperties.getPassword()
                );
    }

    /**
    * This bean is used to create a MongoDatabaseFactory instance.
    * @return MongoDatabaseFactory object.
    */
    @Bean
    @Primary
    public MongoDatabaseFactory mongoDatabaseFactory() {
        return new SimpleMongoClientDatabaseFactory(mongoClient(),
                mongoDBProperties.getDatabase());
    }
    /**
    * This bean is used to create a MongoTemplate instance.
    * @return MongoTemplate object.
    */

    @Bean
    @Primary
    public MongoTemplate mongoTemplate() {

        return new MongoTemplate(mongoDatabaseFactory());
    }
}
