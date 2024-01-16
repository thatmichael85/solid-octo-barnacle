package dist.migration.handlers;

import dist.migration.services.MongoMigrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class MigrationExecutor {
  private static final Logger log = LoggerFactory.getLogger(MigrationExecutor.class);

  private final MongoMigrationService migrationService;

  public MigrationExecutor(MongoMigrationService migrationService) {
    this.migrationService = migrationService;
  }

  public void getCollectionSize(String collectionName) {
    migrationService.getCollectionSize(collectionName).block();
  }

  public void dropDatabase() {
    try {
      migrationService
          .testDestinationConnectivity()
          .flatMap(
              result -> {
                if (result) {
                  return migrationService.dropDatabase();
                } else {
                  return Mono.error(
                      new MigrationExecutorException("Destination Connectivity Test Failed"));
                }
              })
          .block();
    } catch (Exception e) {
      throw new MigrationExecutorException("Drop database Failed");
    }
  }

  public void checkConnectivity() throws MigrationExecutorException {
    try {
      migrationService
          .testSourceConnectivity()
          .flatMap(
              result -> {
                if (result) {
                  return migrationService.testDestinationConnectivity();
                } else {
                  return Mono.error(
                      new MigrationExecutorException("Source Connectivity Test Failed"));
                }
              })
          .flatMap(
              result -> {
                if (!result) {
                  return Mono.error(
                      new MigrationExecutorException("Destination Connectivity Test Failed"));
                }
                return Mono.empty();
              })
          .block();
    } catch (Exception e) {
      if (e instanceof MigrationExecutorException) {
        throw e;
      } else {
        throw new MigrationExecutorException("Uncaught Error during connectivity check", e);
      }
    }
  }

  public void run() {
    try {
      startMigration().block();
    } catch (RuntimeException e) {
      if (e.getCause() instanceof MigrationExecutorException) {
        throw (MigrationExecutorException) e.getCause();
      }
      throw e;
    }
  }

  public void run(String collectionName) {
    try {
      log.info("Migrating: {}", collectionName);
      startMigration(collectionName).block();
    } catch (RuntimeException e) {
      if (e.getCause() instanceof MigrationExecutorException) {
        throw (MigrationExecutorException) e.getCause();
      }
      throw e;
    }
  }

  private Mono<Void> startMigration(String collectionName) {
    return Mono.fromRunnable(() -> log.info("Starting migration..."))
        .then(migrationService.testSourceConnectivity())
        .flatMap(
            result -> {
              if (result) {
                return migrationService.testDestinationConnectivity();
              } else {
                return Mono.error(
                    new MigrationExecutorException("Source Connectivity Test Failed"));
              }
            })
        .flatMap(
            result -> {
              if (result) {
                return migrationService.migrateCollection(collectionName);
              } else {
                return Mono.error(
                    new MigrationExecutorException("Destination Connectivity Test Failed"));
              }
            })
        .doOnSuccess(aVoid -> log.info("Migration completed successfully"))
        .doOnError(e -> log.error("Migration failed", e))
        .doFinally(signalType -> log.info("Migration process ended with signal: " + signalType))
        .then()
        .onErrorMap(
            e -> new MigrationExecutorException("Migration process was interrupted or failed", e));
  }

  private Mono<Void> startMigration() {
    return Mono.fromRunnable(() -> log.info("Starting migration..."))
        .then(migrationService.testSourceConnectivity())
        .flatMap(
            result -> {
              if (result) {
                return migrationService.testDestinationConnectivity();
              } else {
                return Mono.error(
                    new MigrationExecutorException("Source Connectivity Test Failed"));
              }
            })
        .flatMap(
            result -> {
              if (result) {
                return migrationService.migrate();
              } else {
                return Mono.error(
                    new MigrationExecutorException("Destination Connectivity Test Failed"));
              }
            })
        .doOnSuccess(aVoid -> log.info("Migration completed successfully"))
        .doOnError(e -> log.error("Migration failed", e))
        .doFinally(signalType -> log.info("Migration process ended with signal: " + signalType))
        .then()
        .onErrorMap(
            e -> new MigrationExecutorException("Migration process was interrupted or failed", e));
  }
}
