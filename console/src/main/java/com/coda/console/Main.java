package com.coda.console;

import com.coda.core.entities.DataModel;
import com.coda.core.service.DataModelService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.List;

@SpringBootApplication(scanBasePackages = {"com.coda.core", "com.coda.console"})
@EnableMongoRepositories(basePackages = "com.coda.core.repository")
public class Main {

    /**
     * Main method to start the application.
     * @param args command line arguments
     */

    public static void main(final String[] args) {
        SpringApplication.run(Main.class, args);
    }

    /**
     * The runner method is used to run the application.
     * @param dataModelService the dataModel service.
     * @return CommandLineRunner
     */

    @Bean
    public CommandLineRunner runner(final DataModelService dataModelService) {
        return args -> {
            //test extract data from file of
            // the dataModel service method.
            List<DataModel<Object>> dataModel
                    = dataModelService.extractDataFromFile(
                    "data/data.csv");
            System.out.println(dataModel);
            System.out.println(dataModel.get(0).getAttributesMap());
            System.out.println(dataModel.get(1).getAttributesMap());


        };
    }

}
