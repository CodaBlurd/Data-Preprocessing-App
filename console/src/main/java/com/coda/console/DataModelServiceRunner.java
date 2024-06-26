package com.coda.console;

import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class DataModelServiceRunner {

    @Bean
    public CommandLineRunner runner(final DataModelService dataModelService,
                                    final JobLauncher jobLauncher,
                                    final Job importDataJob) {
        return args -> {
            ConsoleMenu consoleMenu = new ConsoleMenu(dataModelService, jobLauncher, importDataJob);
            consoleMenu.displayMenu();
        };
    }
}
