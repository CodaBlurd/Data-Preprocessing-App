package com.coda.core.config;

import com.coda.core.batch.processor.DataModelProcessor;
import com.coda.core.entities.DataModel;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.Constants;
import com.coda.core.util.db.ReaderFactory;
import com.coda.core.util.db.WriterFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.List;

/**
 * This is the configuration class for the batch job.
 * It contains the job, step, reader, writer, processor, and transaction
 * manager beans.
 */
@Slf4j
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    /**
     * JobRepository instance.
     */
    private final JobRepository jobRepository;

    /**
     * PlatformTransactionManager instance.
     */
    private final PlatformTransactionManager transactionManager;

    /**
     * DataModelProcessor instance.
     */
    private final DataModelProcessor dataModelProcessor;

    /**
     * ReaderFactory instance.
     */
    private final ReaderFactory readerFactory;

    /**
     * WriterFactory instance.
     */
    private final WriterFactory writerFactory;

    /**
     * DataSource instance.
     */

    @Setter
    @Qualifier("dataSource")
    private DataSource dataSource;

    /**
     * Constants.DataSourceType instance.
     */

    @Value("${dataSource.type}")
    private Constants.DataSourceType dataSourceType;

    /**
     * Constants.FilePath instance.
     */

    @Value("${file.filePath}")
    private String filePath;

    /**
     * Constants.resourcePath instance.
     */

    @Value("${file.resourcePath}")
    private String resourcePath;

    /**
     *Table name.
     */

    @Value("${dataSource.table.name}")
    private String tableName;

    /**
     * dataSource url.
     */


    @Value("${dataSource.url}")
    private String url;

    /**
     * targetFilePath.
     */

    @Value("${file.targetFilePath}")
    private String targetFilePath;

    /**
     * constructor to inject dependencies.
     * @param repository the job repository
     * @param transactionManager the transaction manager
     * @param processor the data model processor
     * @param dataSource the data source
     * @param rFactory the reader factory
     * @param wFactory the writer factory
     */

    public BatchConfiguration(@Lazy final JobRepository repository,
                              final PlatformTransactionManager transactionManager,
                              final DataModelProcessor processor,
                              final DataSource dataSource,
                              final ReaderFactory rFactory,
                              final WriterFactory wFactory) {
        this.jobRepository = repository;
        this.transactionManager = transactionManager;
        this.dataModelProcessor = processor;
        this.dataSource = dataSource;
        this.readerFactory = rFactory;
        this.writerFactory = wFactory;
    }

    /**
     * Transaction manager bean.
     * @return the transaction manager
     */

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager(this.dataSource);
    }

    /**
     * ItemProcessor bean.
     * @param dataModelRepository the data model repository
     * @return the item processor
     */

    @Bean
    public ItemProcessor<DataModel<Object>, DataModel<Object>> processor(
            final DataModelRepository dataModelRepository) {
        return dataModel -> {
            dataModelProcessor.processAndSaveDataModels(
                    List.of(dataModel),
                    Constants.BATCH_SIZE, dataModelRepository);
            return dataModel;
        };
    }

    /**
     * ItemWriter bean.
     * @return the item writer
     */

    @Bean
    public ItemWriter<DataModel<Object>> writer() {
        return writerFactory.getWriter();
    }

    /**
     * ItemReader bean.
     * @return the item reader
     */

    @Bean
    public ItemReader<DataModel<Object>> reader() {
        return readerFactory.getReader();
    }

    /**
     * Job bean.
     * @param extractStep the extract step
     * @param transformStep the transform step
     * @param loadStep the load step
     * @return the job
     */

    @Bean
    public Job importDataJob(final Step extractStep,
                             final Step transformStep, final Step loadStep) {
        log.info("Job started");
        return new JobBuilder("importDataJob", jobRepository)
                .start(extractStep)
                .next(transformStep)
                .next(loadStep)
                .build();
    }

    /**
     * Extract step bean.
     * @param reader the reader
     * @param writer the writer
     * @return the step
     */

    @Bean
    public Step extractStep(final ItemReader<DataModel<Object>> reader,
                           final ItemWriter<DataModel<Object>> writer) {
        return new StepBuilder("extractStep", jobRepository)
                .<DataModel<Object>, DataModel<Object>>chunk(Constants.BATCH_SIZE, transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    }

    /**
     * Transform step bean.
     * @param processor the processor
     * @param reader the reader
     * @param writer the writer
     * @return the step
     */

    @Bean
    public Step transformStep(final ItemProcessor<DataModel<Object>, DataModel<Object>> processor,
                             final ItemReader<DataModel<Object>> reader,
                             final ItemWriter<DataModel<Object>> writer) {
        return new StepBuilder("transformStep", jobRepository)
                .<DataModel<Object>, DataModel<Object>>chunk(Constants.BATCH_SIZE, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    /**
     * Load step bean.
     * @param writer the writer
     * @param reader the reader
     * @return the step
     */

    @Bean
    public Step loadStep(final ItemWriter<DataModel<Object>> writer,
                       final ItemReader<DataModel<Object>> reader) {
        return new StepBuilder("loadStep", jobRepository)
                .<DataModel<Object>, DataModel<Object>>chunk(Constants.BATCH_SIZE, transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    }
}
