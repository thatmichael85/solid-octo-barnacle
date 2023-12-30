package dist.migration.validators;

import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.CollectionName;
import dist.migration.dtos.DataBaseName;
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

  private void validateDatabaseName(String env, DataBaseName dataBaseName) {
    AppConfigProperties envConfig = configuration.getConfigForEnv(env);
    if (envConfig == null || dataBaseName == null) {
      throw new InputValidatorException("Invalid environment or database name is null");
    }
    if (!envConfig.getValidDatabases().contains(dataBaseName.getValue())) {
      throw new InputValidatorException(
          "Invalid database name: " + dataBaseName.getValue() + " for environment: " + env);
    }
  }

  private void validateCollectionName(String env, CollectionName collectionName) {
    AppConfigProperties envConfig = configuration.getConfigForEnv(env);
    if (envConfig == null
        || collectionName == null
        || !envConfig.getValidCollections().contains(collectionName.name())) {
      throw new InputValidatorException(
          "Invalid collection name: " + collectionName + " for environment: " + env);
    }
  }
}
