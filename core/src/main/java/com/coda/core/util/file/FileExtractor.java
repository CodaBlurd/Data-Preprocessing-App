package com.coda.core.util.file;

import com.coda.core.entities.DataModel;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * FileExtractor is an interface that
 * provides methods to read and
 * write data from files.
 * <p>It provides methods to
 * read data from files using Apache CSV library.
 * </p>
 */
public interface FileExtractor {

        /**
         * Reads data from a file using Apache CSV library.
         * <p>This method reads data from a file
         * and returns a list of DataModel objects.
         * </p>
         * @param filePath The path of the file to read data from.
         * @return A list of DataModel objects.
         * @throws IOException If an I/O error occurs.
         */

        List<DataModel<Object>> readDataWithApacheCSV(
                String filePath) throws IOException;

        /**
         * Reads data from an input stream using Apache CSV library.
         * <p>This method reads data from an input stream
         * and returns a list of DataModel objects.
         * </p>
         * @param inputStream The input stream to read data from.
         * @return A list of DataModel objects.
         * @throws IOException If an I/O error occurs.
         */


        List<DataModel<Object>> readDataWithApacheCSV(
                InputStream inputStream) throws IOException;


        /**
         * Checks if a file exists.
         * <p>This method checks if a
         * file exists at the specified path.
         * </p>
         * @param filePath The path of the file to check.
         * @return True if the file exists, false otherwise.
         */
        boolean exists(String filePath);

        /**
         * Checks if a file can be read.
         * <p>This method checks if a file
         * can be read at the specified path.
         *  </p>
         * @param filePath The path of the file to check.
         * @return True if the file can be read, false otherwise.
         */
        boolean canRead(String filePath);

        /**
         * Checks if a file can be written to.
         * <p>This method checks if a
         * file can be written to at the specified path.
         * </p>
         * @param filePath The path of the file to check.
         * @return True if the file can be written to, false otherwise.
         */
        boolean canWrite(String filePath);

        /**
         * Writes data to a file using Apache CSV library.
         * <p>This method writes data to a
         * file using Apache CSV library.
         * </p>
         * @param dataModels The list of DataModel objects to write.
         * @param filePath The path of the file to write data to.
         * @throws IOException If an I/O error occurs.
         */
        void writeDataWithApacheCSV(
                List<DataModel<Object>> dataModels,
                String filePath) throws IOException;
}
