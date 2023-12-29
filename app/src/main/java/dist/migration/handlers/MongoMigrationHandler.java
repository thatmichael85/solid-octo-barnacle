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
import dist.migration.services.*;
import dist.migration.validators.InputValidator;
import java.io.InputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class MongoMigrationHandler implements RequestHandler<InputDto, String> {
  private static final Logger log = LoggerFactory.getLogger(MongoMigrationHandler.class);
  private static final String CONFIG_FILE = "/appconfig.yml";

  @Override
  public String handleRequest(InputDto input, Context context) {
    MDC.put("AWSRequestId", context.getAwsRequestId());
    log.info("Received {}", input);
    Configuration config = loadConfig();
    InputValidator validator = new InputValidator(config);
    validator.validate(input);
    AwsSecretsServiceImpl awsSecretsService = new AwsSecretsServiceImpl();
    AppConfigProperties appConfig = config.getConfigForEnv(input.getEnv());
    startMigration(appConfig, input.getCollectionName().toString(), awsSecretsService);
    MDC.clear();
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
    AwsSecretServiceLocal awsSecretServiceLocal = new AwsSecretServiceLocal();
    startMigration(devConfig, collectionName, awsSecretServiceLocal);
  }

  private static void startMigration(
      AppConfigProperties appConfigProperties,
      String collectionName,
      AwsSecretsService awsSecretsService) {
    log.info("Starting migration...");
    // Extract configuration details for source and destination
    MongoMigrationService migrationService =
        createMongoMigrationService(appConfigProperties, collectionName, awsSecretsService);

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

  private static MongoMigrationService createMongoMigrationService(
      AppConfigProperties appConfigProperties,
      String collectionName,
      AwsSecretsService awsSecretsService) {
    String sourceUri = appConfigProperties.getSourceUrl();
    String sourceDatabase = appConfigProperties.getSourceDatabase();
    String sourceUsername = awsSecretsService.getSecret(appConfigProperties.getSourceUserNameArn());
    String sourcePassword =
        awsSecretsService.getSecret(appConfigProperties.getSourceUserPasswordArn());

    String destUri = appConfigProperties.getDestinationUrl();
    String destDatabase = appConfigProperties.getDestinationDatabase();
    String destUsername =
        awsSecretsService.getSecret(appConfigProperties.getDestinationUserNameArn());
    String destPassword =
        awsSecretsService.getSecret(appConfigProperties.getDestinationUserPasswordArn());

    // Instantiate MongoMigrationService
    return new MongoMigrationService(
        sourceUri,
        sourceDatabase,
        sourceUsername,
        sourcePassword,
        destUri,
        destDatabase,
        destUsername,
        destPassword,
        collectionName);
  }
}
