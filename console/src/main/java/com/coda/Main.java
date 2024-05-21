package com.coda;

import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;

import java.util.List;

@SpringBootApplication
@Slf4j
 public class Main {
    /**
     * The resource loader.
     */
    @Autowired
    private ResourceLoader resourceLoader;


    /**
     * The main method is used to start the application.
     * @param args The arguments that are passed to the application.
     */
    public static void main(final String[] args) {

        SpringApplication.run(Main.class, args);
    }

    /**
     * The runner method is used to run the application.
     * @param dataModelService The data model service.
     * @return The command line runner object.
     */
    @Bean
    public CommandLineRunner runner(final DataModelService dataModelService) {
        return args -> {
            String inputFilePath = "file.csv";
            String outputFilePath = "fileOutput.csv";
            try {
                List<DataModel<Object>> dataModels
                        = dataModelService.extractDataFromFile(inputFilePath);
                dataModelService.loadDataToCSV(dataModels, outputFilePath);
                log.info("ETL process completed successfully."
                        + " Output written to {}", outputFilePath);
            } catch (DataExtractionException e) {
                log.error("Failed to process ETL due to: {}",
                        e.getMessage());
            }
        };
    }


}

