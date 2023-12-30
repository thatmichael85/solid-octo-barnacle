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

  private Mono<Void> startMigration() {
    return Mono.fromRunnable(() -> log.info("Starting migration..."))
        .then(migrationService.testSourceConnectivity())
        .flatMap(
            result -> {
              if (result) {
                // Only proceed to test destination connectivity if source connectivity test passes
                return migrationService.testDestinationConnectivity();
              } else {
                // If source connectivity test fails, throw an error and stop the chain
                return Mono.error(new MigrationExecutorException("Source Connectivity Test Failed"));
              }
            })
        .flatMap(
            result -> {
              if (result) {
                // Proceed to migration only if both connectivity tests pass
                return migrationService.migrate();
              } else {
                return Mono.error(new MigrationExecutorException("Destination Connectivity Test Failed"));
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
