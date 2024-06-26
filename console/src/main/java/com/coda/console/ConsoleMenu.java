package com.coda.console;

import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;

import java.util.Scanner;

@Slf4j
public final class ConsoleMenu {

    private final DataModelService dataModelService;
    private final JobLauncher jobLauncher;
    private final Job importDataJob;
    private final Scanner scanner;

    public ConsoleMenu(DataModelService dataModelService,
                       JobLauncher jobLauncher,
                       Job importDataJob) {
        this.dataModelService = dataModelService;
        this.jobLauncher = jobLauncher;
        this.importDataJob = importDataJob;
        this.scanner = new Scanner(System.in);
    }

    public void displayMenu() {
        while (true) {
            log.info("\n------ Data Preprocessing App ------");
            log.info("1. Extract data from MySQL database");
            log.info("2. Exit");
            System.out.print("Enter your choice: ");

            String choice = scanner.nextLine();

            switch (choice) {
                case "1":
                    extractDataFromDatabase();
                    break;
                case "2":
                    log.info(" -> Exiting ....");
                    scanner.close();
                    return;
                default:
                    log.error("Invalid choice... Please try again");
            }
        }
    }

    private void extractDataFromDatabase() {
        log.info("Extracting data from MySQL database");
        log.info("Enter the database connection details:");

        System.out.print("Enter database URL: ");
        String dbUrl = scanner.nextLine();

        System.out.print("Enter database username: ");
        String dbUsername = scanner.nextLine();

        System.out.print("Enter database password: ");
        String dbPassword = scanner.nextLine();

        System.out.print("Enter database name: ");
        String dbName = scanner.nextLine();

        System.out.print("Enter table name: ");
        String tableName = scanner.nextLine();

        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("dbUrl", dbUrl)
                    .addString("dbUsername", dbUsername)
                    .addString("dbPassword", dbPassword)
                    .addString("dbName", dbName)
                    .addString("tableName", tableName)
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution jobExecution = jobLauncher.run(importDataJob, jobParameters);
            log.info("Job Status: {}", jobExecution.getStatus());
            log.info("Job Start Time: {}", jobExecution.getStartTime());
            log.info("Job Exit Status: {}", jobExecution.getExitStatus());

        } catch (Exception e) {
            log.error("Failed to extract data from MySQL database: {}", e.getMessage(), e);
        }
    }
}
