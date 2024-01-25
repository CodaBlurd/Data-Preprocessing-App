package com.coda.core.util.file;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to extract data from a file
 * <p> This class is used to extract data from a file </p>
 * Implements FileExtractor interface
 * @see FileExtractor
 * @see DataParser
 */
@Slf4j
@Service
public class FileExtractorImpl implements FileExtractor{
    private final DataParser dataParser;

    public FileExtractorImpl(DataParser dataParser) {
        this.dataParser = dataParser;
    }

    /**
     * This method is used to read data from a file
     * @param filePath the path of the file
     * @return List<DataModel<Object>>
     */
    public List<DataModel<Object>> readData(String filePath) {
        //== local variables ==
        List<DataModel<Object>> dataModels = new ArrayList<>();
        //== read data from the file ==
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    DataModel<Object> dataModel = dataParser.parseLine(line);
                    dataModels.add(dataModel);
                } catch (IllegalArgumentException e) {
                    log.error("Failed to parse line: " + line, e);
                }
            }
        } catch (IOException e) {
            log.error("Failed to read file: " + filePath, e);
        }
        return dataModels;
    }
}
