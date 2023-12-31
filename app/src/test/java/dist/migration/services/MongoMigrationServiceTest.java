package dist.migration.services;

import static org.mockito.Mockito.*;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class MongoMigrationServiceTest {

  @Mock private MongoClient mockSourceClient;
  @Mock private MongoClient mockDestClient;
  @Mock private MongoDatabase mockDatabase;
  @Mock private MongoCollection<Document> mockCollection;

  private AutoCloseable closeable;
  private MongoMigrationService service;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockSourceClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockDestClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockDatabase.getCollection(anyString())).thenReturn(mockCollection);
    service =
        new MongoMigrationService(
            mockSourceClient, "sourceDb", mockDestClient, "destDb", "testCollection");
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  // Test for getCollectionSize
  @Test
  void getCollectionSize_Success() {
    when(mockCollection.countDocuments()).thenReturn(Mono.just(10L));

    StepVerifier.create(service.getCollectionSize("testCollection"))
        .expectNext(10L)
        .verifyComplete();
  }

  @Test
  void getCollectionSize_Error() {
    when(mockCollection.countDocuments())
        .thenReturn(Mono.error(new RuntimeException("Database error")));

    StepVerifier.create(service.getCollectionSize("testCollection"))
        .verifyErrorMatches(
            e -> e instanceof RuntimeException && e.getMessage().equals("Database error"));
  }

  // Test for dropDestinationCollection
  @Test
  void dropDestinationCollection_Success() {
    when(mockCollection.drop()).thenReturn(Mono.empty());

    StepVerifier.create(service.dropDestinationCollection("testCollection")).verifyComplete();
  }

  @Test
  void dropDestinationCollection_Error() {
    when(mockCollection.drop()).thenReturn(Mono.error(new RuntimeException("Drop error")));

    StepVerifier.create(service.dropDestinationCollection("testCollection"))
        .verifyErrorMatches(
            e -> e instanceof RuntimeException && e.getMessage().equals("Drop error"));
  }

  // Test for testSourceConnectivity
  @Test
  void testSourceConnectivity_Success() {
    when(mockDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.just(new Document("ok", 1.0)));

    StepVerifier.create(service.testSourceConnectivity()).expectNext(true).verifyComplete();
  }

  @Test
  void testSourceConnectivity_Error() {
    when(mockDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.error(new RuntimeException("Connectivity error")));

    StepVerifier.create(service.testSourceConnectivity()).expectNext(false).verifyComplete();
  }

  // Test for testDestinationConnectivity
  @Test
  void testDestinationConnectivity_Success() {
    when(mockDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.just(new Document("ok", 1.0)));

    StepVerifier.create(service.testDestinationConnectivity()).expectNext(true).verifyComplete();
  }

  @Test
  void testDestinationConnectivity_Error() {
    when(mockDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.error(new RuntimeException("Connectivity error")));

    StepVerifier.create(service.testDestinationConnectivity()).expectNext(false).verifyComplete();
  }
}
