package dist.migration.validators;

import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.InputDto;

public class InputValidator {

  private final Configuration configuration;

  public InputValidator(Configuration configuration) {
    this.configuration = configuration;
  }

  public void validate(InputDto inputDto) {
    validateEnvironment(inputDto.getEnv());
    validateDatabaseName(inputDto.getEnv(), inputDto.getDataBaseName());
    validateCollectionName(inputDto.getEnv(), inputDto.getCollectionName());
  }

  private void validateEnvironment(String env) {
    if (!("dev".equals(env) || "qa".equals(env) || "prod".equals(env))) {
      throw new InputValidatorException("Invalid environment: " + env);
    }
  }

  private void validateDatabaseName(String env, String dataBaseName) {
    AppConfigProperties envConfig = configuration.getConfigForEnv(env);
    if (envConfig == null || dataBaseName == null) {
      throw new InputValidatorException("Invalid environment or database name is null");
    }
    if (!envConfig.getValidDatabases().contains(dataBaseName)) {
      throw new InputValidatorException(
          "Invalid database name: " + dataBaseName + " for environment: " + env);
    }
  }

  private void validateCollectionName(String env, String collectionName) {
    AppConfigProperties envConfig = configuration.getConfigForEnv(env);
    if (envConfig == null
        || collectionName == null
        || !envConfig.getValidCollections().contains(collectionName)) {
      throw new InputValidatorException(
          "Invalid collection name: " + collectionName + " for environment: " + env);
    }
  }
}
