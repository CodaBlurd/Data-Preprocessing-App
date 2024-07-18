//package com.coda.web.controller;
//
//import com.coda.core.dtos.ConnectionDetails;
//import com.coda.core.entities.DataAttributes;
//import com.coda.core.entities.DataModel;
//import com.coda.core.service.DataModelService;
//import com.coda.core.util.db.DatabaseExtractor;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mockito;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
//import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
//import org.springframework.boot.test.mock.mockito.MockBean;
//import org.springframework.http.MediaType;
//import org.springframework.test.context.junit.jupiter.SpringExtension;
//import org.springframework.test.web.servlet.MockMvc;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import static org.mockito.Mockito.when;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
//import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
//
//@WebMvcTest(DataController.class)
//@ExtendWith(SpringExtension.class)
//@AutoConfigureMockMvc
//public class DataControllerTest {
//
//    @Autowired
//    private MockMvc mockMvc;
//
//    @Autowired
//    private ObjectMapper objectMapper;
//
//    @MockBean
//    private DataModelService dataModelService;
//
//    @Test
//    public void extractDataFromMySQL_Success() throws Exception {
//        // Arrange
//        String url = "/api/v1/data/extract/mysql";
//        String type = "mysql";
//        String tableName = "testTable";
//        ConnectionDetails connectionDetails = new ConnectionDetails("jdbc:mysql://localhost:3306/testdb", "user", "pass");
//
//        List<DataModel<Object>> expectedDataModels = new ArrayList<>();
//        DataAttributes<Object> attribute1 = new DataAttributes<>("column1", "value1", "VARCHAR", Object.class);
//        DataModel<Object> dataModel = new DataModel<>();
//        dataModel.setAttributesMap(Map.of(attribute1.getAttributeName(), attribute1));
//        DatabaseExtractor extractor = Mockito.mock(DatabaseExtractor.class);
//
//        expectedDataModels.add(dataModel);
//        when(dataModelService.extractDataFromTable(connectionDetails, type, tableName))
//                .thenReturn(expectedDataModels);
//
//        String jsonBody = objectMapper.writeValueAsString(connectionDetails);
//
//        // Act & Assert
//        mockMvc.perform(post(url)
//                        .contentType(MediaType.APPLICATION_JSON)
//                        .content(jsonBody)
//                        .param("type", type)
//                        .param("tableName", tableName))
//                .andExpect(status().isOk())
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
//                .andDo(result -> {
//                    String responseBody = result.getResponse().getContentAsString();
//                    System.out.println("Response Body: " + responseBody);
//                })
//                .andExpect(jsonPath("$").isArray())
//                .andExpect(jsonPath("$[0].attributesMap.column1.value").value("value1"));
//    }
//
//    @Test
//    public void extractDataFromMySQL_InvalidInput() throws Exception {
//        // Arrange
//        String url = "/api/v1/data/extract/mysql";
//        String type = "";
//        String tableName = "";
//        ConnectionDetails connectionDetails = new ConnectionDetails("", "", "");
//        String jsonBody = objectMapper.writeValueAsString(connectionDetails);
//
//        // Act & Assert
//        mockMvc.perform(post(url)
//                        .contentType(MediaType.APPLICATION_JSON)
//                        .content(jsonBody)
//                        .param("type", type)
//                        .param("tableName", tableName))
//                .andExpect(status().isBadRequest());
//    }
//}
//
