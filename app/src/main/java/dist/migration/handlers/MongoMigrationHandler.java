package dist.migration.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.InputDto;
import dist.migration.services.MongoMigrationService;
import java.io.InputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoMigrationHandler implements RequestHandler<InputDto, String> {
  private static final Logger log = LoggerFactory.getLogger(MongoMigrationHandler.class);
  private static final String CONFIG_FILE = "/appconfig.yml";
  private static final String databaseName = "HELLO_DATABASE";
  private static String arn =
      "arn:aws:secretsmanager:us-east-1:000000000000:secret:test-secret-vozPFj";

  @Override
  public String handleRequest(InputDto input, Context context) {
    Configuration config = loadConfig();
    AppConfigProperties appConfig = config.getConfigForEnv(input.getEnv());

    return appConfig.toString();
  }

  private static Configuration loadConfig() {
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    try (InputStream inputStream = MongoMigrationHandler.class.getResourceAsStream(CONFIG_FILE)) {
      Map<String, AppConfigProperties> configMap =
          yamlMapper.readValue(
              inputStream, new TypeReference<Map<String, AppConfigProperties>>() {});
      Configuration configuration = new Configuration();
      configuration.setEnvironments(configMap);
      return configuration;
    } catch (Exception e) {
      throw new RuntimeException("Error loading configuration", e);
    }
  }

  // Just for local testing
  public static void main(String[] args) {
    startMigration();
  }

  private static void startMigration() {
    log.info("Starting migration...");

    // Load the configuration
    Configuration config = loadConfig();
    AppConfigProperties devConfig = config.getConfigForEnv("dev");

    // Extract configuration details for source and destination
    String sourceUri = devConfig.getSourceUrl();
    String sourceDatabase = devConfig.getSourceDatabase();
    String sourceUsername = devConfig.getSourceUserNameArn();
    String sourcePassword = devConfig.getSourceUserPasswordArn();

    String destUri = devConfig.getDestinationUrl();
    String destDatabase = devConfig.getDestinationDatabase();
    String destUsername = devConfig.getDestinationUserNameArn();
    String destPassword = devConfig.getDestinationUserPasswordArn();

    // Assuming collectionName is defined somewhere in your configuration or as a constant
    String collectionName = "yourCollectionName";

    // Instantiate MongoMigrationService
    MongoMigrationService migrationService = new MongoMigrationService(
            sourceUri, sourceDatabase, sourceUsername, sourcePassword,
            destUri, destDatabase, destUsername, destPassword,
            collectionName
    );
    migrationService.testSourceConnectivity().block();
    migrationService.testDestinationConnectivity().block();

    try {
      migrationService
          .migrate()
          .doOnSuccess(aVoid -> log.info("Migration completed successfully"))
          .doOnError(e -> log.error("Migration failed", e))
          .doFinally(signalType -> log.info("Migration process ended with signal: " + signalType))
          .block(); // This will block until the migration is complete
    } catch (Exception e) {
      // Exception handling if block() throws an exception (e.g., InterruptedException)
      log.error("Migration process was interrupted or failed", e);
    }
  }
}
