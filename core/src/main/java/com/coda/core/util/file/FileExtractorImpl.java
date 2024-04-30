package com.coda.core.util.file;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class FileExtractorImpl implements FileExtractor {

    public FileExtractorImpl() {
        // Constructor remains empty if no initialization is required.
    }

    @Override
    public List<DataModel<Object>> readDataWithApacheCSV(String filePath) throws IOException {
        List<DataModel<Object>> dataModels = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(Paths.get(filePath));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
            for (CSVRecord record : csvParser) {
                DataModel<Object> dataModel = getObjectDataModel(record);
                dataModels.add(dataModel);
            }
        } catch (IOException e) {
            log.error("Failed to read file: {}", filePath, e);
            throw e; // Rethrow the exception to allow callers to handle it
        }
        return dataModels;
    }

    @Override
    public boolean exists(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) return false;
        File file = new File(filePath);
        return file.exists() && file.isFile();
    }

    @Override
    public boolean canRead(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) return false;
        File file = new File(filePath);
        return file.canRead() && file.isFile();
    }


    // Private helper method remains the same
    private static DataModel<Object> getObjectDataModel(CSVRecord record) {
        Map<String, DataAttributes<Object>> attributes = new HashMap<>();
        record.toMap().forEach((key, value) -> {
            DataAttributes<Object> attribute = new DataAttributes<>(key, value, "Object", Object.class);
            attributes.put(key, attribute);
        });
        DataModel<Object> dataModel = new DataModel<>();
        dataModel.setAttributesMap(attributes);
        return dataModel;
    }
}

