package com.coda.core.util.timestamps;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.time.Instant;
import java.time.format.DateTimeParseException;

@Component
@Slf4j
public class FileTimestampStorage {

    /**
     * The file name to store the last extracted timestamp.
     */
    private static final String TIMESTAMP_FILE = "last_extracted_timestamp.txt";

    /**
     * Get the last extracted timestamp from the file.
     * @return the last extracted timestamp
     */

    public Instant getLastExtractedTimestamp() {
        File file = new File(TIMESTAMP_FILE);
        if (!file.exists()) {
            // If file doesn't exist, create it with the epoch start time
            file.getParentFile().mkdirs();
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            updateLastExtractedTimestamp(Instant.EPOCH);
            return Instant.EPOCH;
        }

        try (BufferedReader reader
                     = new BufferedReader(new FileReader(file))) {
            String timestampStr = reader.readLine();
            return Instant.parse(timestampStr);
        } catch (IOException | DateTimeParseException e) {
            return Instant.EPOCH;
        }
    }

    /**
     * updateLastExtractedTimestamp().
     * writes the last extracted timestamp to the file.
     * @param timestamp the last extracted timestamp.
     */

    public void updateLastExtractedTimestamp(final Instant timestamp) {

        try (BufferedWriter writer
                     = new BufferedWriter(new FileWriter(TIMESTAMP_FILE))) {
            writer.write(timestamp.toString());
        } catch (IOException e) {
            log.error("Error updating last extracted "
                    + "timestamp: {}", e.getMessage());
            throw new RuntimeException("Error updating last "
                    + "extracted timestamp", e);
        }
    }
}
