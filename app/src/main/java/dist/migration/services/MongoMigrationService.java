package dist.migration.services;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MongoMigrationService {

  private static final Logger logger = LoggerFactory.getLogger(MongoMigrationService.class);
  public static final int BATCH_SIZE = 200;
  private final String sourceUri;
  private final String destUri;
  private final String sourceDbName;
  private final String destDbName;
  private final String collectionName;

  public MongoMigrationService(
      String sourceHost,
      String sourceDatabase,
      String sourceUsername,
      String sourcePassword,
      String destHost,
      String destinationDatabase,
      String destinationUsername,
      String destinationPassword,
      String collectionName) {
    this.sourceUri = createMongoUri(sourceHost, sourceUsername, sourcePassword);
    this.destUri = createMongoUri(destHost, destinationUsername, destinationPassword);
    this.sourceDbName = sourceDatabase;
    this.destDbName = destinationDatabase;
    this.collectionName = collectionName;
  }

  private String createMongoUri(String host, String username, String password) {
    String credentials = username + ":" + password + "@";
    return "mongodb://" + credentials + host;
  }

  public Mono<Void> migrate() {
    long startTime = System.currentTimeMillis(); // Start timing

    MongoClient sourceClient = MongoClients.create(sourceUri);
    MongoClient destClient = MongoClients.create(destUri);

    MongoDatabase sourceDb = sourceClient.getDatabase(sourceDbName);
    MongoCollection<Document> sourceCollection = sourceDb.getCollection(collectionName);

    MongoDatabase destDb = destClient.getDatabase(destDbName);
    MongoCollection<Document> destCollection = destDb.getCollection(collectionName);

    AtomicLong totalDocumentsMigrated = new AtomicLong(0);
    AtomicLong totalSizeMigrated = new AtomicLong(0);

    return Flux.from(sourceCollection.find())
        .buffer(BATCH_SIZE) // Buffer documents for batch insertion
        .flatMap(
            batch -> {
              long batchTotalSize =
                  batch.stream()
                      .mapToLong(doc -> doc.toJson().getBytes(StandardCharsets.UTF_8).length)
                      .sum();
              totalSizeMigrated.addAndGet(batchTotalSize);
              return destCollection.insertMany(batch);
            })
        .onErrorMap(
            ex ->
                new MongoMigrationServiceException(
                    "Something went wrong on inserting this batch...", ex))
        .doOnNext(
            insertManyResult -> {
              long count =
                  totalDocumentsMigrated.addAndGet(insertManyResult.getInsertedIds().size());
              logger.info("Migrated " + count + " documents so far...");
            })
        .then(
            Mono.zip(
                Mono.from(sourceCollection.countDocuments()),
                Mono.from(destCollection.countDocuments())))
        .flatMap(
            t -> {
              logger.info(
                  "Completed migration. Source total Doc count is: {}, Destination total Doc count is: {}",
                  t.getT1(),
                  t.getT2());
              return Mono.empty();
            })
        .then()
        .doOnTerminate(
            () -> {
              long endTime = System.currentTimeMillis();
              long totalTimeInSeconds = (endTime - startTime) / 1000;
              double totalSizeInGB = totalSizeMigrated.get() / (1024.0 * 1024.0 * 1024.0);

              logger.info(
                  "Migration completed: Total Time: "
                      + totalTimeInSeconds
                      + " seconds, Total Documents: "
                      + totalDocumentsMigrated.get()
                      + ", Database: "
                      + destDbName
                      + ", Collection: "
                      + collectionName
                      + ", Total Size: "
                      + totalSizeInGB
                      + " GB");
              logger.info("Source URI: " + sourceUri);
              logger.info("Destination URI: " + destUri);

              sourceClient.close();
              destClient.close();
            });
  }

  public Mono<Boolean> testSourceConnectivity() {
    return testConnection(sourceUri, "Source", sourceDbName);
  }

  public Mono<Boolean> testDestinationConnectivity() {
    return testConnection(destUri, "Destination", destDbName);
  }

  private Mono<Boolean> testConnection(String uri, String label, String dbName) {
    MongoClient client = MongoClients.create(uri);
    MongoDatabase database = client.getDatabase(dbName);

    Document pingCommand = new Document("ping", 1);
    Mono<Document> commandMono = Mono.from(database.runCommand(pingCommand));

    return commandMono
        .doOnSuccess(
            doc ->
                logger.info(
                    label + " Database connection successful. Ping response: " + doc.toJson()))
        .doOnError(e -> logger.error(label + " Database connection failed", e))
        .thenReturn(true)
        .onErrorResume(
            e -> {
              logger.error("Error during ping command: ", e);
              return Mono.just(false);
            })
        .doFinally(signalType -> client.close());
  }
}
