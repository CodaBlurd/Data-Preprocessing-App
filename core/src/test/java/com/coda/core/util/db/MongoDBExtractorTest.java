package com.coda.core.util.db;

import com.coda.core.config.MongoDBConfig;
import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.util.timestamps.FileTimestampStorage;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class MongoDBExtractorTest {

    @Mock
    private MongoDBConnectionFactory mongoDBConnectionFactory;

    @Mock
    private MongoDBConfig mongoDBConfig;

    @Mock
    private FileTimestampStorage fileTimestampStorage;

    @InjectMocks
    private MongoDBExtractor mongoDBExtractor;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        mongoDBExtractor = new MongoDBExtractor(mongoDBConnectionFactory, mongoDBConfig, fileTimestampStorage);
    }

    @Test
    public void testReadData() throws Exception {
        // Mocking dependencies
        MongoClient mockClient = mock(MongoClient.class);
        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> mockCollection = mock(MongoCollection.class);
        FindIterable<Document> mockFindIterable = mock(FindIterable.class);
        MongoCursor<Document> mockCursor = mock(MongoCursor.class);
        Document mockDocument = mock(Document.class);

        when(mongoDBConfig.mongoClient()).thenReturn(mockClient);
        when(mongoDBConnectionFactory.getConnection(mockClient, "testDb")).thenReturn(mockDatabase);
        when(mockDatabase.getCollection("testTable")).thenReturn(mockCollection);
        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        when(mockCursor.hasNext()).thenReturn(false);
        Instant mockTimestamp = Instant.now();
        when(fileTimestampStorage.getLastExtractedTimestamp()).thenReturn(mockTimestamp);

        // Execute the method
        Map<String, DataModel<Document>> result = mongoDBExtractor.readData("testDb", "testTable", "testUrl");

        // Verify interactions
        ArgumentCaptor<Bson> argumentCaptor = ArgumentCaptor.forClass(Bson.class);
        verify(mockCollection).find(argumentCaptor.capture());
        Bson capturedFilter = argumentCaptor.getValue();

        assertEquals(Filters.gt("updatedAt", mockTimestamp), capturedFilter);
        verify(fileTimestampStorage).updateLastExtractedTimestamp(any(Instant.class));
        assertEquals(0, result.size()); // Assuming no documents are returned in this test case
    }

    @Test
    public void testReadDataWithDocuments() throws Exception {
    // Mocking dependencies
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDatabase = mock(MongoDatabase.class);
    MongoCollection<Document> mockCollection = mock(MongoCollection.class);
    FindIterable<Document> mockFindIterable = mock(FindIterable.class);
    MongoCursor<Document> mockCursor = mock(MongoCursor.class);
    Document valueDocument = new Document("key1", "value1");
    Document mockDocument = new Document("_id", "1").append("updatedAt", Instant.now()).append("key1", valueDocument);

    when(mongoDBConfig.mongoClient()).thenReturn(mockClient);
    when(mongoDBConnectionFactory.getConnection(mockClient, "testDb")).thenReturn(mockDatabase);
    when(mockDatabase.getCollection("testTable")).thenReturn(mockCollection);
    when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
    when(mockFindIterable.iterator()).thenReturn(mockCursor);
    when(mockCursor.hasNext()).thenReturn(true, false);
    when(mockCursor.next()).thenReturn(mockDocument);
    Instant mockTimestamp = Instant.now();
    when(fileTimestampStorage.getLastExtractedTimestamp()).thenReturn(mockTimestamp);

    // Execute the method
    Map<String, DataModel<Document>> result = mongoDBExtractor.readData("testDb",
            "testTable", "testUrl");

    // Verify results
    assertEquals(1, result.size());
    DataModel<Document> dataModel = result.get("1");
    assertNotNull(dataModel);

    DataAttributes<Document> dataAttributes = dataModel.getAttributesMap().get("key1");
    assertNotNull(dataAttributes);
    assertEquals("value1", dataAttributes.getValue().getString("key1"));

    verify(fileTimestampStorage).updateLastExtractedTimestamp(any(Instant.class));
}


    @Test
    public void testLoadData() throws Exception {
        // Mocking
        MongoClient mockClient = mock(MongoClient.class);
        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection<Document> mockCollection = mock(MongoCollection.class);

        when(mongoDBConfig.mongoClient()).thenReturn(mockClient);
        when(mongoDBConnectionFactory.getConnection(mockClient, "testDb")).thenReturn(mockDatabase);
        when(mockDatabase.getCollection("testTable")).thenReturn(mockCollection);

        // Create a sample DataModel
        Map<String, DataModel<Document>> dataModels = new HashMap<>();
        Map<String, DataAttributes<Document>> attributes = new HashMap<>();
        Document sampleDocument = new Document("key1", "value1");
        attributes.put("key1", new DataAttributes<>("key1", sampleDocument,
                "Document", Document.class));
        DataModel<Document> dataModel = new DataModel<>();
        dataModel.setAttributesMap(attributes);
        dataModels.put("id1", dataModel);

        // Execute the method
        mongoDBExtractor.loadData(dataModels, "testDb", "testTable", "testUrl");

        // Verify interactions
        ArgumentCaptor<List<Document>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockCollection, times(1)).insertMany(argumentCaptor.capture());
        List<Document> capturedDocuments = argumentCaptor.getValue();

        assertNotNull(capturedDocuments);
        assertEquals(1, capturedDocuments.size());
        assertEquals("value1", capturedDocuments.get(0).get("key1", Document.class).getString("key1"));

        verify(fileTimestampStorage, times(1)).updateLastExtractedTimestamp(any(Instant.class));
    }
}
