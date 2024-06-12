package com.coda.core.config;

import com.coda.core.batch.load.DataModelWriter;
import com.coda.core.batch.extraction.DataModelReader;
import com.coda.core.batch.processor.DataModelProcessor;
import com.coda.core.entities.DataModel;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.service.DataModelService;
import com.coda.core.util.Constants;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

/**
 * This is the configuration class for the batch job.
 * It contains the job, step, reader, writer, processor, and transaction
 * manager beans.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    /**
     * This is the job repository for the batch job.
     * It is used to manage the batch job.
     */
    @Autowired
    private JobRepository jobRepository;

    /**
     * This is the transaction manager for the batch job.
     * It is used to manage the transactions for the batch job.
     * Roll back the transaction if any exception occurs. etc
     */

    @Autowired
    private PlatformTransactionManager transactionManager;

    /**
     * This is the DataModelService object.
     * It is used to extract and transform the data from the file.
     */

    @Autowired
    private DataModelService dataModelService;

    /**
     * This is the DataSourceType object.
     */

    @Value("${dataSourceType}")
    private Constants.DataSourceType dataSourceType;

    /**
     * This is the file path for the data source.
     */

    @Value("${filePath}")
    private String filePath;

    /**
     * This is the resource path for the data source.
     */

    @Value("${resourcePath}")
    private String resourcePath;

    /**
     * This is the database type for the data source.
     */

    @Value("${dbType}")
    private String dbType;

    /**
     * This is the table name for the data source.
     */

    @Value("${tableName}")
    private String tableName;

    /**
     * This is the database name for the data source.
     */

    @Value("${dbName}")
    private String dbName;

    /**
     * This is the url for the data source.
     */

    @Value("${url}")
    private String url;

    /**
     * This is the DataModelProcessor object.
     */

    @Autowired
    private DataModelProcessor dataModelProcessor;

    /**
     * This is the processor bean for the batch job.
     * It is used to process the data.
     * It also saves the data to the database.
     * @param dataModelRepository the DataModelRepository object.
     * @return the ItemProcessor object.
     */

    @Bean
    public ItemProcessor<DataModel<Object>, DataModel<Object>> processor(
            final DataModelRepository dataModelRepository) {
        return dataModel -> {
            dataModelProcessor
                    .processAndSaveDataModels(List.of(dataModel),
                    Constants.BATCH_SIZE, dataModelRepository);
            return dataModel;
        };
    }

    /**
     * This is the writer bean for the batch job.
     * It is used to write the data to the database.
     * @return the ItemWriter object.
     */

    @Bean
    public ItemWriter<DataModel<?>> writer() {
        return new DataModelWriter(dataModelService, dataSourceType,
                filePath, resourcePath, dbType,
                tableName, dbName, url);
    }

    /**
     * This is the reader bean for the batch job.
     * It is used to read the data from the file.
     * @return the ItemReader object.
     */

    @Bean
    public ItemReader<DataModel<?>> reader() {
        return new DataModelReader(dataModelService, dataSourceType,
                filePath, resourcePath, dbType,
                tableName, dbName, url);
    }

    /**
     * This is the job bean for the batch job.
     * It is used to import the data from the sources.
     * @param extractStep the Step object for the extract step.
     * @param transformStep the Step object for the transform step.
     * @param loadStep the Step object for the load step.
     * @return the Job object.
     */
    @Bean
    public Job importDataJob(final Step extractStep,
                             final Step transformStep,
                             final Step loadStep) {
        return new JobBuilder("importDataJob", jobRepository)
                .start(extractStep)
                .next(transformStep)
                .next(loadStep)
                .build();
    }

    /**
     * This is the step bean for the extract step.
     * It is used to extract the data from the sources.
     * @param reader the ItemReader object.
     * @param writer the ItemWriter object.
     * @return the Step object.
     */

    @Bean
    public Step extractStep(final ItemReader<DataModel<?>> reader,
                            final ItemWriter<DataModel<?>> writer) {
        return new StepBuilder("extractStep", jobRepository)
                .<DataModel<?>, DataModel<?>>chunk(Constants.BATCH_SIZE,
                        transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    }

    /**
     * This is the step bean for the transform step.
     * It is used to transform the data.
     * @param processor the ItemProcessor object.
     * @return the Step object.
     */

    @Bean
    public Step transformStep(final ItemProcessor<DataModel<Object>,
            DataModel<Object>> processor) {
        return new StepBuilder("transformStep", jobRepository)
                .<DataModel<Object>,
                        DataModel<Object>>chunk(Constants.BATCH_SIZE,
                        transactionManager)
                .processor(processor)
                .build();
    }

    /**
     * This is the step bean for the load step.
     * It is used to load the data to the database.
     * @param writer the ItemWriter object.
     * @return the Step object.
     */

    @Bean
    public Step loadStep(final ItemWriter<DataModel<?>> writer) {
        return new StepBuilder("loadStep", jobRepository)
                .<DataModel<?>,
                        DataModel<?>>chunk(Constants.BATCH_SIZE,
                        transactionManager)
                .writer(writer)
                .build();
    }
}
