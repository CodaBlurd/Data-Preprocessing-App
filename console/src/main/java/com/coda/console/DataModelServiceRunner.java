package com.coda.console;

import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class DataModelServiceRunner {

    /**
     * The runner method to execute data model service methods.
     * @param dataModelService the dataModel service
     * @return CommandLineRunner instance
     */
    @Bean
    public CommandLineRunner runner(final DataModelService dataModelService) {
        return args -> {
            ConsoleMenu consoleMenu = new ConsoleMenu(dataModelService);
            consoleMenu.displayMenu();
        };
    }
}
