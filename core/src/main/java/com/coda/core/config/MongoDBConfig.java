package com.coda.core.config;

import com.coda.core.util.db.MongoDBUtil;
import com.mongodb.client.MongoClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

@Configuration
public class MongoDBConfig {

    /**
     * MongoDB client bean.
     * this bean is used to create a MongoClient instance.
     * @return MongoClient object.
     */

    @Bean
    public MongoClient mongoClient() {
        return MongoDBUtil.createMongoClient();
    }

    /**
    * This bean is used to create a MongoDatabaseFactory instance.
    * @return MongoDatabaseFactory object.
    */
    @Bean
    public MongoDatabaseFactory mongoDatabaseFactory() {
        return new SimpleMongoClientDatabaseFactory(mongoClient(), "admin");
    }
    /**
    * This bean is used to create a MongoTemplate instance.
    * @return MongoTemplate object.
    */

    @Bean
    public MongoTemplate mongoTemplate() {
        return new MongoTemplate(mongoDatabaseFactory());
    }
}
