package com.coda.console;

import com.coda.core.entities.DataModel;
import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

@Slf4j
public final class ConsoleMenu {

    /**
     * The dataModel service class.
     */
    private final DataModelService dataModelService;

    /**
     * Scanner for reading in user inputs.
     */
    private final Scanner scanner;

    /**
     * Constructor.
     *
     * @param service the dataModel service class.
     */
    public ConsoleMenu(final DataModelService service) {
        this.dataModelService = service;
        this.scanner = new Scanner(System.in);
    }

    /**
     * Displays the app menu to the user.
     */
    public void displayMenu() {
        while (true) {
            log.info("\n------ Data Preprocessing App ------");
            log.info("1. Extract data from a file");
            log.info("2. Extract data from database");
            log.info("3. Load data to CSV");
            log.info("4. Exit");
            System.out.print("Enter your choice: ");

            String choice = scanner.nextLine();

            switch (choice) {
                case "1":
                    extractDataFromFile();
                    break;
                case "2":
                    extractDataFromDatabase();
                    break;
                case "3":
                    loadDataToCSV();
                    break;
                case "4":
                    log.info(" -> Exiting ....");
                    scanner.close();
                    return;
                default:
                    log.error("Invalid choice... Please try again");
            }
        }
    }

    /**
     * Uses the dataModel service to extract data from file resource.
     */

    private void extractDataFromFile() {
        log.info("Enter the path to the data file: ");
        String filePath = scanner.nextLine();
        if (filePath == null || filePath.isEmpty()) {
            log.error("File path cannot be null or empty");
            return;
        }
        Path path = Path.of(filePath);
        if (!Files.exists(path) || !Files.isReadable(path)) {
            log.error("File does not exist or is not readable");
            return;
        }

        try {
            List<DataModel<Object>> dataModels
                    = dataModelService
                    .extractDataFromFileOnFileSystem(filePath);
            dataModels.stream().filter(Objects::nonNull)
                    .forEach(dataModel -> log.info("Extracted DataModel: {}",
                            dataModel));
            log.info("Extracted DataModels: {}", dataModels);
            log.info("Extracted {} DataModels from file: {}",
                    dataModels.size(), filePath);
        } catch (Exception e) {
            log.error("Failed to extract data from file: {}",
                    e.getMessage(), e);
        }
    }

    /**
     * Uses the dataModel service to extract data from database source.
     */

    private void extractDataFromDatabase() {


    }

    /**
     * Uses the dataModel service to load data to file system.
     */
    private void loadDataToCSV() {


    }
}
