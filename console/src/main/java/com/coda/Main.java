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
    @Autowired
    private ResourceLoader resourceLoader;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Bean
    public CommandLineRunner runner(DataModelService dataModelService) {
        return args -> {
            String inputFilePath = "file.csv"; // Ensure this path is valid in your resources
            String outputFilePath = "fileOutput.csv";
            try {
                List<DataModel<Object>> dataModels = dataModelService.extractDataFromFile(inputFilePath);
                dataModelService.loadDataToCSV(dataModels, outputFilePath);
                log.info("ETL process completed successfully. Output written to {}", outputFilePath);
            } catch (DataExtractionException e) {
                log.error("Failed to process ETL due to: {}", e.getMessage());
            }
        };
    }


}
