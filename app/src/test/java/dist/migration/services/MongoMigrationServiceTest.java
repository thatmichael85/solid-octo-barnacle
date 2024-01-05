package dist.migration.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class MongoMigrationServiceTest {

  @Mock
  private MongoClient mockSourceClient;
  @Mock
  private MongoClient mockDestClient;
  @Mock
  private MongoDatabase mockSourceDatabase;
  @Mock
  private MongoDatabase mockDestDatabase;
  @Mock
  private MongoCollection<Document> mockSourceCollection;
  @Mock
  private MongoCollection<Document> mockDestCollection;

  private AutoCloseable closeable;
  private MongoMigrationService service;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockSourceClient.getDatabase(anyString())).thenReturn(mockSourceDatabase);
    when(mockDestClient.getDatabase(anyString())).thenReturn(mockDestDatabase);
    when(mockSourceDatabase.getCollection(anyString())).thenReturn(mockSourceCollection);
    when(mockDestDatabase.getCollection(anyString())).thenReturn(mockDestCollection);
    service = new MongoMigrationService(
        mockSourceClient, "sourceDb", mockDestClient, "destDb", "testCollection");
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void getCollectionSize_Success() {
    when(mockDestCollection.countDocuments()).thenReturn(Mono.just(10L));
    StepVerifier.create(service.getCollectionSize("testCollection"))
        .expectNext(10L)
        .verifyComplete();
  }

  @Test
  void getCollectionSizeError() {
    when(mockDestCollection.countDocuments())
        .thenReturn(Mono.error(new RuntimeException("Database error")));
    StepVerifier.create(service.getCollectionSize("testCollection"))
        .verifyErrorMatches(
            e -> e instanceof RuntimeException && e.getMessage().equals("Database error"));
  }

  @Test
  void dropDestinationCollectionSuccess() {
    when(mockDestCollection.drop()).thenReturn(Mono.empty());
    StepVerifier.create(service.dropDestinationCollection("testCollection")).verifyComplete();
  }

  @Test
  void dropDestinationCollectionError() {
    when(mockDestCollection.drop()).thenReturn(Mono.error(new RuntimeException("Drop error")));
    StepVerifier.create(service.dropDestinationCollection("testCollection"))
        .verifyErrorMatches(
            e -> e instanceof RuntimeException && e.getMessage().equals("Drop error"));
  }

  @Test
  void testSourceConnectivitySuccess() {
    when(mockSourceDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.just(new Document("ok", 1.0)));
    StepVerifier.create(service.testSourceConnectivity()).expectNext(true).verifyComplete();
  }

  @Test
  void testSourceConnectivityError() {
    when(mockSourceDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.error(new RuntimeException("Connectivity error")));
    StepVerifier.create(service.testSourceConnectivity()).expectNext(false).verifyComplete();
  }

  @Test
  void testDestinationConnectivity_Success() {
    when(mockDestDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.just(new Document("ok", 1.0)));
    StepVerifier.create(service.testDestinationConnectivity()).expectNext(true).verifyComplete();
  }

  @Test
  void testDestinationConnectivity_Error() {
    when(mockDestDatabase.runCommand(any(Document.class)))
        .thenReturn(Mono.error(new RuntimeException("Connectivity error")));
    StepVerifier.create(service.testDestinationConnectivity()).expectNext(false).verifyComplete();
  }

  @Test
  void migrateSuccess() {
    // Mocking the publisher - very difficult to do
    FindPublisher<Document> findPublisherMock = mock(FindPublisher.class);
    doAnswer(
        invocation -> {
          Subscriber<Document> s = invocation.getArgument(0);
          s.onSubscribe(
              new Subscription() {
                private boolean isCancelled = false;

                @Override
                public void request(long n) {
                  if (!isCancelled && n > 0) {
                    s.onNext(new Document("key", "value1"));
                    s.onComplete();
                  }
                }

                @Override
                public void cancel() {
                  isCancelled = true;
                }
              });
          return null;
        })
        .when(findPublisherMock)
        .subscribe(any(Subscriber.class));

    ListIndexesPublisher<Document> listIndexesPublisherMock = mock(ListIndexesPublisher.class);
    doAnswer(invocation -> {
      Subscriber<Document> s = invocation.getArgument(0);
      s.onSubscribe(new Subscription() {
        private boolean isCancelled = false;

        @Override
        public void request(long n) {
          if (!isCancelled && n > 0) {
            s.onComplete();
          }
        }

        @Override
        public void cancel() {
          isCancelled = true;
        }
      });
      return null;
    }).when(listIndexesPublisherMock).subscribe(any());

    when(mockSourceDatabase.listCollectionNames()).thenReturn(Flux.just("collection1"));
    when(mockSourceCollection.listIndexes()).thenReturn(listIndexesPublisherMock);
    when(mockSourceCollection.find()).thenReturn(findPublisherMock);

    Map<Integer, BsonValue> insertedIds = new HashMap<>();
    insertedIds.put(0, new BsonObjectId());
    insertedIds.put(1, new BsonObjectId());
    insertedIds.put(2, new BsonObjectId());
    InsertManyResult insertManyResult = InsertManyResult.acknowledged(insertedIds);
    when(mockDestCollection.insertMany(anyList())).thenReturn(Mono.just(insertManyResult));

    when(mockSourceCollection.countDocuments()).thenReturn(Mono.just(3L));
    when(mockDestCollection.countDocuments()).thenReturn(Mono.just(3L));

    StepVerifier.create(service.migrate()).expectComplete().verify();
  }

}
