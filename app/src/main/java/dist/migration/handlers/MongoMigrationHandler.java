package dist.migration.handlers;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.mongodb.reactivestreams.client.MongoClient;
import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.EventType;
import dist.migration.dtos.InputDto;
import dist.migration.dtos.ResponseDto;
import dist.migration.factories.MongoClientFactory;
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
    try {
      MDC.put("AWSRequestId", context.getAwsRequestId());
      log.info("Received {}, AWS timeLimit {} and memoryLimit {}", input, context.getRemainingTimeInMillis(), context.getMemoryLimitInMB());
      Configuration config = loadConfig();
      InputValidator validator = new InputValidator(config);
      validator.validate(input);
      AwsSecretsServiceImpl awsSecretsService = new AwsSecretsServiceImpl();
      MongoMigrationService migrationService =
          createMongoMigrationService(config, input, awsSecretsService);
      MigrationExecutor executor = new MigrationExecutor(migrationService);
      switch (input.getEventType()) {
        case dropCollection -> executor.dropCollection(input.getCollectionName());
        case checkConnectivity -> executor.checkConnectivity();
        case getCollectionSize -> executor.getCollectionSize(input.getCollectionName());
        case executeMigration -> executor.run();
      }
      MDC.clear();
      return ResponseDto.builder()
          .awsContext(context)
          .dataBaseName(input.getDataBaseName())
          .collectionName(input.getCollectionName())
          .eventType(input.getEventType())
          .result("Successful")
          .build()
          .toString();
    } catch (Exception e) {
      return ResponseDto.builder()
          .awsContext(context)
          .dataBaseName(input.getDataBaseName())
          .collectionName(input.getCollectionName())
          .eventType(input.getEventType())
          .result("Failed with: " + e.getMessage())
          .build()
          .toString();
    }
  }

  private static MongoMigrationService createMongoMigrationService(
      Configuration config, InputDto input, AwsSecretsService awsSecretsService) {

    AppConfigProperties appConfigProperties = config.getConfigForEnv(input.getEnv());

    String sourceHost = appConfigProperties.getSourceUrl();
    String sourceDatabase = input.getDataBaseName();
    String sourceUsername = awsSecretsService.getSecret(appConfigProperties.getSourceUserNameArn());
    String sourcePassword =
        awsSecretsService.getSecret(appConfigProperties.getSourceUserPasswordArn());

    String destHost = appConfigProperties.getDestinationUrl();
    String destinationDatabase = input.getDataBaseName();
    String destinationUsername =
        awsSecretsService.getSecret(appConfigProperties.getDestinationUserNameArn());
    String destinationPassword =
        awsSecretsService.getSecret(appConfigProperties.getDestinationUserPasswordArn());
    String collectionName = input.getCollectionName();

    MongoClient sourceClient =
        MongoClientFactory.createClient(sourceHost, sourceUsername, sourcePassword);
    MongoClient destClient =
        MongoClientFactory.createClient(destHost, destinationUsername, destinationPassword);

    return new MongoMigrationService(
        sourceClient, sourceDatabase, destClient, destinationDatabase, collectionName);
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
    testInput.setDataBaseName("DefaultDatabase");
    testInput.setCollectionName("yourCollectionName");
    testInput.setEventType(EventType.executeMigration);
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

  private class LocalContext implements Context {

      @Override
      public String getAwsRequestId() {
        return "LOCALTESTING";
      }

      @Override
      public ClientContext getClientContext() {
        return null;
      }

      @Override
      public String getFunctionName() {
        // TODO Auto-generated method stub
        return "LOCALTESTING";
      }

      @Override
      public String getFunctionVersion() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public CognitoIdentity getIdentity() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getInvokedFunctionArn() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getLogGroupName() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getLogStreamName() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public LambdaLogger getLogger() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public int getMemoryLimitInMB() {
        // TODO Auto-generated method stub
        return 10;
      }

      @Override
      public int getRemainingTimeInMillis() {
        // TODO Auto-generated method stub
        return 123407734;
      }
      
    }
}
