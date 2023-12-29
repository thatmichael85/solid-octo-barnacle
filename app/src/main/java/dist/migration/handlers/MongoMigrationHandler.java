package dist.migration.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.InputDto;
import dist.migration.services.AwsSecretsService;
import dist.migration.services.MongoMigrationService;
import dist.migration.services.MongoMigrationServiceException;
import dist.migration.validators.InputValidator;
import java.io.InputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoMigrationHandler implements RequestHandler<InputDto, String> {
  private static final Logger log = LoggerFactory.getLogger(MongoMigrationHandler.class);
  private static final String CONFIG_FILE = "/appconfig.yml";

  @Override
  public String handleRequest(InputDto input, Context context) {
    log.info("Received {}", input);
    Configuration config = loadConfig();
    InputValidator validator = new InputValidator(config);
    validator.validate(input);
    AppConfigProperties appConfig = config.getConfigForEnv(input.getEnv());
    startMigration(config, appConfig, input.getCollectionName().toString());
    return context.toString();
  }

  private static Configuration loadConfig() {
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    yamlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try (InputStream inputStream = MongoMigrationHandler.class.getResourceAsStream(CONFIG_FILE)) {
      Map<String, AppConfigProperties> configMap =
          yamlMapper.readValue(inputStream, new TypeReference<>() {});
      Configuration configuration = new Configuration();
      configuration.setEnvironments(configMap);
      return configuration;
    } catch (Exception e) {
      throw new RuntimeException("Error loading configuration", e);
    }
  }

  // Just for local testing
  public static void main(String[] args) {
    // Load the configuration
    Configuration config = loadConfig();
    AppConfigProperties devConfig = config.getConfigForEnv("dev");
    String collectionName = "yourCollectionName";
    startMigration(config, devConfig, collectionName);
  }

  private static void startMigration(
      Configuration configuration, AppConfigProperties appConfigProperties, String collectionName) {
    log.info("Starting migration...");
    // Extract configuration details for source and destination
    String sourceUri = appConfigProperties.getSourceUrl();
    String sourceDatabase = appConfigProperties.getSourceDatabase();
    String sourceUsername = AwsSecretsService.getSecret(appConfigProperties.getSourceUserNameArn());
    String sourcePassword =
        AwsSecretsService.getSecret(appConfigProperties.getSourceUserPasswordArn());

    String destUri = appConfigProperties.getDestinationUrl();
    String destDatabase = appConfigProperties.getDestinationDatabase();
    String destUsername =
        AwsSecretsService.getSecret(appConfigProperties.getDestinationUserNameArn());
    String destPassword =
        AwsSecretsService.getSecret(appConfigProperties.getDestinationUserPasswordArn());

    // Instantiate MongoMigrationService
    MongoMigrationService migrationService =
        new MongoMigrationService(
            sourceUri,
            sourceDatabase,
            sourceUsername,
            sourcePassword,
            destUri,
            destDatabase,
            destUsername,
            destPassword,
            collectionName);

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
      throw new MongoMigrationServiceException("Migration process was interrupted or failed", e);
    }
  }
}
