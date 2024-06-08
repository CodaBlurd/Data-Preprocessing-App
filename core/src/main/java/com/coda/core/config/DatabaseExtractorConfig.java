package com.coda.core.config;

import com.coda.core.util.db.MongoDBConnectionFactory;
import com.coda.core.util.db.MongoDBExtractor;
import com.coda.core.util.db.MySQLExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class DatabaseExtractorConfig {

    /**
     * mySQLExtractor().
     * mySQLExtractor is a bean that is
     * used to extract data from a MySQL database.
     * @return MySQLExtractor
     */
    @Bean
    public MySQLExtractor mySQLExtractor() {

        return new MySQLExtractor();
    }

    /**
     * mongoDBExtractor().
     * mongoDBExtractor is a bean that is
     * used to extract data from a MongoDB database.
     * @param mongoDBConnectionFactory The connection factory
     *  used to connect to the database
     * @param config The configuration for the database
     * @return MongoDBExtractor
     */

    @Bean
    @Lazy
    public MongoDBExtractor mongoDBExtractor(
            final MongoDBConnectionFactory mongoDBConnectionFactory,
            final MongoDBConfig config) {

        return new MongoDBExtractor(mongoDBConnectionFactory, config);
    }

    // more db config
}
