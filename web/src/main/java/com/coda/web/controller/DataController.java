package com.coda.web.controller;

import com.coda.core.dtos.ConnectionDetails;
import com.coda.core.service.DataModelService;
import com.coda.core.service.ETLService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.coda.web.util.ApiMapping.BASE_URL_ETL;
import static com.coda.web.util.ApiMapping.MYSQL_ETL;

@Slf4j
@RestController
@RequestMapping(BASE_URL_ETL)
public class DataController {

    /**
     * DataModelService instance.
     */
    private final DataModelService dataModelService;

    private final ETLService etlService;

    /**
     * Constructor to inject dependencies.
     * @param dataModelService the data model service
     */

    public DataController(final DataModelService dataModelService,
                          final ETLService etlService) {
        this.dataModelService = dataModelService;
        this.etlService = etlService;
    }
//
//    @Operation(summary = "Extract data from MySQL and process",
//            description = "Extract and process data from MySQL using provided connection details")
//    @ApiResponses(value = {
//            @ApiResponse(responseCode = "200", description = "Data successfully extracted"),
//            @ApiResponse(responseCode = "400", description = "Invalid input"),
//            @ApiResponse(responseCode = "401", description = "Unauthorized")
//    })
//    @PostMapping(MYSQL_EXTRACT)
//    public ResponseEntity<List<DataModel<Object>>> extractDataFromMySQL(
//            @RequestBody ConnectionDetails connectionDetails,
//            @RequestParam String type,
//            @RequestParam String tableName) {
//        List<DataModel<Object>> data
//                = dataModelService.extractDataFromTable(connectionDetails, type, tableName);
//        return ResponseEntity.ok(data);
//    }

    @Operation(summary = "Perform Etl process for MYSQL")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Etl process completed successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input"),
            @ApiResponse(responseCode = "401", description = "Unauthorized")
    })
    @PostMapping(MYSQL_ETL)
    public ResponseEntity<Void> performEtlProcessForMySQL(
            @RequestBody ConnectionDetails connectionDetails,
            @RequestParam String sourceTableName,
            @RequestParam String targetTableName) {
        etlService.performETLProcess(connectionDetails,
                "mysql",
                sourceTableName, targetTableName,
                "mysql");
        return ResponseEntity.ok().build();
    }
}
