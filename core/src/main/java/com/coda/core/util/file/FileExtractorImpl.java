package com.coda.core.util.file;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.ReadFromFileException;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.Reader;
import java.io.Writer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
public final class FileExtractorImpl implements FileExtractor {

    // file implementation
    @Override
    public List<DataModel<Object>> readDataWithApacheCSV(final String filePath)
            throws IOException {
        try (InputStream inputStream = new FileInputStream(filePath)) {
            return readDataWithApacheCSV(inputStream);
        }
    }

    // Implementation for InputStream
    @Override
    public List<DataModel<Object>>
    readDataWithApacheCSV(final InputStream inputStream) {
        List<DataModel<Object>> dataModels = new ArrayList<>();
        try (Reader reader =
                     new BufferedReader(new InputStreamReader(inputStream));
             CSVParser csvParser =
                     new CSVParser(reader,
                             CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord record : csvParser) {
                DataModel<Object> dataModel =
                        createDataModelFromCsvRecord(record);
                dataModels.add(dataModel);
            }
        } catch (IOException e) {
            log.error("Failed to read data from input stream", e);
            throw new ReadFromFileException(e.getMessage(),
                    ErrorType.READ_ERROR);
        }
        return dataModels;
    }

    @Override
    public boolean exists(final String filePath) {
        return Optional.ofNullable(filePath)
                .filter(path -> !path.trim().isEmpty())
                .map(File::new)
                .map(File::exists)
                .orElse(false);
    }

    @Override
    public boolean canRead(final String filePath) {
        return Optional.ofNullable(filePath)
                .filter(path -> !path.trim().isEmpty())
                .map(File::new)
                .map(File::canRead)
                .orElse(false);
    }

    @Override
    public boolean canWrite(final String filePath) {
        return Optional.ofNullable(filePath)
                .filter(path -> !path.trim().isEmpty())
                .map(File::new)
                .map(file -> (file.exists() && file.canWrite())
                        || (!file.exists()
                        &&
                        file.getParentFile().canWrite()))
                .orElse(false);
    }

    @Override
    public void writeDataWithApacheCSV(
            final List<DataModel<Object>> dataModels,
            final String filePath) throws IOException {
        if (dataModels.isEmpty()) {
            log.error("No data models provided to write to CSV.");
            throw new ReadFromFileException(
                    "No data models provided to write to CSV.",
                    ErrorType.NO_DATA);
        }

        Path path = Paths.get(filePath);
        try (Writer writer = Files.newBufferedWriter(path);
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.withHeader(
                             extractHeaders(dataModels)))) {
            for (DataModel<Object> dataModel : dataModels) {
                List<String> record =
                        dataModel.getAttributesMap().values().stream()
                        .map(attr -> Optional.ofNullable(attr.getValue())
                                .orElse(""))
                        .map(Object::toString)
                        .collect(Collectors.toList());
                csvPrinter.printRecord(record);
            }
            csvPrinter.flush();
        } catch (IOException e) {
            log.error("Failed to write data to file: {}", filePath, e);
            throw e;
        }
    }

    // Helper methods
    private DataModel<Object> createDataModelFromCsvRecord(
            final CSVRecord record) {
        Map<String, DataAttributes<Object>> attributes =
                record.toMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new DataAttributes<>(e.getKey(), e.getValue(),
                                "Object", Object.class)));
        DataModel<Object> dataModel = new DataModel<>();
        dataModel.setAttributesMap(attributes);
        return dataModel;
    }

    private String[] extractHeaders(final List<DataModel<Object>> dataModels) {
        return dataModels.get(0)
                .getAttributesMap()
                .keySet().toArray(new String[0]);
    }
}
