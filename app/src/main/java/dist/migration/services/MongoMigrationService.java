package dist.migration.services;

import com.mongodb.reactivestreams.client.MongoClient;
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

  private final MongoClient sourceClient;
  private final MongoClient destClient;
  private final String sourceDbName;
  private final String destDbName;
  private final String collectionName;

  public MongoMigrationService(
      MongoClient sourceClient,
      String sourceDatabase,
      MongoClient destClient,
      String destinationDatabase,
      String collectionName) {
    this.sourceClient = sourceClient;
    this.destClient = destClient;
    this.sourceDbName = sourceDatabase;
    this.destDbName = destinationDatabase;
    this.collectionName = collectionName;
  }

  public Mono<Void> migrate() {
    long startTime = System.currentTimeMillis();

    MongoDatabase sourceDb = sourceClient.getDatabase(sourceDbName);
    MongoCollection<Document> sourceCollection = sourceDb.getCollection(collectionName);

    MongoDatabase destDb = destClient.getDatabase(destDbName);
    MongoCollection<Document> destCollection = destDb.getCollection(collectionName);

    AtomicLong totalDocumentsMigrated = new AtomicLong(0);
    AtomicLong totalSizeMigrated = new AtomicLong(0);

    return Flux.from(sourceCollection.find())
        .buffer(BATCH_SIZE)
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
        .doOnTerminate(() -> cleanUp(startTime, totalDocumentsMigrated, totalSizeMigrated));
  }

  private void cleanUp(
      long startTime, AtomicLong totalDocumentsMigrated, AtomicLong totalSizeMigrated) {
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

    if (sourceClient != null) {
      sourceClient.close();
    }
    if (destClient != null) {
      destClient.close();
    }
  }

  public Mono<Void> dropDestinationCollection(String collectionName) {
    MongoDatabase destDb = destClient.getDatabase(destDbName);
    MongoCollection<Document> destCollection = destDb.getCollection(collectionName);

    return Mono.from(destCollection.drop())
        .doOnSuccess(
            aVoid ->
                logger.info(
                    "Collection '"
                        + collectionName
                        + "' dropped successfully in destination database"))
        .doOnError(
            e ->
                logger.error(
                    "Error dropping collection '" + collectionName + "' in destination database",
                    e))
        .then()
        .doFinally(
            signalType -> {
              destClient.close();
            });
  }

  public Mono<Long> getCollectionSize(String collectionName) {
    MongoDatabase database = destClient.getDatabase(destDbName);
    MongoCollection<Document> collection = database.getCollection(collectionName);

    return Mono.from(collection.countDocuments())
            .doOnSuccess(docCount ->
                    logger.info("Collection {} has {} documents", collectionName, docCount));
  }

  public Mono<Boolean> testSourceConnectivity() {
    return testConnection(sourceClient, "Source", sourceDbName);
  }

  public Mono<Boolean> testDestinationConnectivity() {
    return testConnection(destClient, "Destination", destDbName);
  }

  private Mono<Boolean> testConnection(MongoClient client, String label, String dbName) {
    MongoDatabase database = client.getDatabase(dbName);
    Document pingCommand = new Document("ping", 1);
    Mono<Document> commandMono = Mono.from(database.runCommand(pingCommand));

    return commandMono
        .map(
            doc -> {
              logger.info(
                  label + " Database connection successful. Ping response: " + doc.toJson());
              return true;
            })
        .onErrorResume(
            e -> {
              logger.error(label + " Database connection failed", e);
              return Mono.just(false);
            });
  }
}
