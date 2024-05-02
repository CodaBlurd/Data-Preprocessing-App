package com.coda.core.util.file;

import com.coda.core.entities.DataModel;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface FileExtractor {

        // Existing file-based implementation
        List<DataModel<Object>> readDataWithApacheCSV(String filePath) throws IOException;

        // New method to handle InputStream directly
        List<DataModel<Object>> readDataWithApacheCSV(InputStream inputStream) throws IOException;

        // Existing methods
        boolean exists(String filePath);
        boolean canRead(String filePath);
        boolean canWrite(String filePath);
        void writeDataWithApacheCSV(List<DataModel<Object>> dataModels, String filePath) throws IOException;
}
