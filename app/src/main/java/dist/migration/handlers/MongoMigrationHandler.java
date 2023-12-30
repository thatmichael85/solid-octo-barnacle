package dist.migration.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.CollectionName;
import dist.migration.dtos.DataBaseName;
import dist.migration.dtos.EventType;
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
    MongoMigrationService migrationService =
        createMongoMigrationService(config, input, awsSecretsService);
    MigrationExecutor executor = new MigrationExecutor(migrationService);
    executor.run();
    MDC.clear();
    return context.toString();
  }

  private static MongoMigrationService createMongoMigrationService(
      Configuration config, InputDto input, AwsSecretsService awsSecretsService) {

    AppConfigProperties appConfigProperties = config.getConfigForEnv(input.getEnv());

    String sourceHost = awsSecretsService.getSecret(appConfigProperties.getSourceUrl());
    String sourceDatabase = awsSecretsService.getSecret(input.getDataBaseName().getValue());
    String sourceUsername = awsSecretsService.getSecret(appConfigProperties.getSourceUserNameArn());
    String sourcePassword =
        awsSecretsService.getSecret(appConfigProperties.getSourceUserPasswordArn());
    String destHost = awsSecretsService.getSecret(appConfigProperties.getDestinationUrl());
    String destinationDatabase = awsSecretsService.getSecret(input.getDataBaseName().getValue());
    String destinationUsername =
        awsSecretsService.getSecret(appConfigProperties.getDestinationUserNameArn());
    String destinationPassword =
        awsSecretsService.getSecret(appConfigProperties.getDestinationUserPasswordArn());
    String collectionName = input.getCollectionName().toString();

    return new MongoMigrationService(
        sourceHost,
        sourceDatabase,
        sourceUsername,
        sourcePassword,
        destHost,
        destinationDatabase,
        destinationUsername,
        destinationPassword,
        collectionName);
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
    Configuration config = loadConfig();
    InputDto testInput = new InputDto();
    testInput.setDataBaseName(DataBaseName.DEFAULT_DATA_BASE);
    testInput.setCollectionName(CollectionName.yourCollectionName);
    testInput.setEventType(EventType.EXECUTE_MIGRATION);
    testInput.setEnv("dev");
    InputValidator validator = new InputValidator(config);
    validator.validate(testInput);
    AwsSecretsService awsSecretsService = new AwsSecretServiceLocal();
    MongoMigrationService migrationService =
        createMongoMigrationService(config, testInput, awsSecretsService);
    MigrationExecutor executor = new MigrationExecutor(migrationService);
    executor.run();
    log.info("Migration process completed in local testing");
  }
}
