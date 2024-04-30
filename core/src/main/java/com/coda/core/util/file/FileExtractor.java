package com.coda.core.util.file;

import com.coda.core.entities.DataModel;

import java.io.IOException;
import java.util.List;

public interface FileExtractor {

        /**
         * This method is used to read data from a file
         * @param filePath the path of the file
         * @return List<DataModel<Object>>
        */
        List<DataModel<Object>> readDataWithApacheCSV(String filePath) throws IOException;
        boolean exists(String filePath);
        boolean canRead(String filePath);
}
