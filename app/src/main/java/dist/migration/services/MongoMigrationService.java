package dist.migration.services;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MongoMigrationService {

  private static final Logger logger = LoggerFactory.getLogger(MongoMigrationService.class);
  public static final int BATCH_SIZE = 400;

  private final MongoClient sourceClient;
  private final MongoClient destClient;
  private final String sourceDbName;
  private final String destDbName;

  public MongoMigrationService(
      MongoClient sourceClient,
      String sourceDatabase,
      MongoClient destClient,
      String destinationDatabase) {
    this.sourceClient = sourceClient;
    this.destClient = destClient;
    this.sourceDbName = sourceDatabase;
    this.destDbName = destinationDatabase;
  }

  public Mono<Void> migrateCollection(String collectionName) {
    long startTime = System.currentTimeMillis();
    MongoDatabase sourceDb = sourceClient.getDatabase(sourceDbName);
    MongoDatabase destDb = destClient.getDatabase(destDbName);

    MongoCollection<Document> sourceCollection = sourceDb.getCollection(collectionName);
    MongoCollection<Document> destCollection = destDb.getCollection(collectionName);

    AtomicLong totalDocumentsMigrated = new AtomicLong(0);
    AtomicLong totalSizeMigrated = new AtomicLong(0);

    return Flux.from(sourceCollection.listIndexes())
        .collectList()
        .flatMap(indexes -> createIndices(destCollection, indexes))
        .thenMany(Flux.from(sourceCollection.find()))
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
                    "Error during data migration for collection: " + collectionName, ex))
        .doOnNext(
            insertManyResult -> {
              long count =
                  totalDocumentsMigrated.addAndGet(insertManyResult.getInsertedIds().size());
              logger.info(
                  "Migrated " + count + " documents so far in collection: " + collectionName);
            })
        .then(
            Mono.fromRunnable(
                () ->
                    cleanUp(
                        startTime,
                        destDbName,
                        collectionName,
                        totalDocumentsMigrated,
                        totalSizeMigrated)))
        .then()
        .doOnTerminate(
            () -> {
              logger.info("Closing source and dest clients");
              sourceClient.close();
              destClient.close();
              logger.info(
                  "Migration completed in {} seconds.",
                  (System.currentTimeMillis() - startTime) / 1000);
            });
  }

  public Mono<Void> migrate() {
    long startTime = System.currentTimeMillis();
    MongoDatabase sourceDb = sourceClient.getDatabase(sourceDbName);
    MongoDatabase destDb = destClient.getDatabase(destDbName);

    AtomicLong totalDocumentsMigrated = new AtomicLong(0);
    AtomicLong totalSizeMigrated = new AtomicLong(0);

    return Flux.from(sourceDb.listCollectionNames())
        .flatMap(
            collectionName -> {
              MongoCollection<Document> sourceCollection = sourceDb.getCollection(collectionName);
              MongoCollection<Document> destCollection = destDb.getCollection(collectionName);

              return Flux.from(sourceCollection.listIndexes())
                  .collectList()
                  .flatMap(indexes -> createIndices(destCollection, indexes))
                  .thenMany(Flux.from(sourceCollection.find()))
                  .buffer(BATCH_SIZE)
                  .flatMap(
                      batch -> {
                        long batchTotalSize =
                            batch.stream()
                                .mapToLong(
                                    doc -> doc.toJson().getBytes(StandardCharsets.UTF_8).length)
                                .sum();
                        totalSizeMigrated.addAndGet(batchTotalSize);
                        return destCollection.insertMany(batch);
                      })
                  .onErrorMap(
                      ex ->
                          new MongoMigrationServiceException(
                              "Error during data migration for collection: " + collectionName, ex))
                  .doOnNext(
                      insertManyResult -> {
                        long count =
                            totalDocumentsMigrated.addAndGet(
                                insertManyResult.getInsertedIds().size());
                        logger.info(
                            "Migrated "
                                + count
                                + " documents so far in collection: "
                                + collectionName);
                      })
                  .then(
                      Mono.fromRunnable(
                          () ->
                              cleanUp(
                                  startTime,
                                  destDbName,
                                  collectionName,
                                  totalDocumentsMigrated,
                                  totalSizeMigrated)));
            },
            2) // 2 collections at a time at most.
        .then()
        .doOnTerminate(
            () -> {
              logger.info("Closing source and dest clients");
              sourceClient.close();
              destClient.close();
              logger.info(
                  "Everything completed in {}.", (System.currentTimeMillis() - startTime) / 1000);
            });
  }

  private Mono<Void> createIndices(MongoCollection<Document> collection, List<Document> indexes) {
    return Flux.fromIterable(indexes)
        .flatMap(
            index -> {
              Document indexKeys = (Document) index.get("key");
              IndexOptions options = new IndexOptions();
              // Set other index options as needed
              return Mono.from(collection.createIndex(indexKeys, options));
            })
        .then();
  }

  private void cleanUp(
      long startTime,
      String destDbName,
      String collectionName,
      AtomicLong totalDocumentsMigrated,
      AtomicLong totalSizeMigrated) {
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
  }

  public Mono<Void> dropDatabase() {
    MongoDatabase destDb = destClient.getDatabase(destDbName);

    return Mono.from(destDb.drop())
        .doOnSuccess(
            aVoid ->
                logger.info(
                    "Database '" + destDbName + "' dropped successfully in destination cluster"))
        .doOnError(
            e ->
                logger.error(
                    "Error dropping database '" + destDbName + "' in destination cluster", e))
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
        .doOnSuccess(
            docCount -> logger.info("Collection {} has {} documents", collectionName, docCount));
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
