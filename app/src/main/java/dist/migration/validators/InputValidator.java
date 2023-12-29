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
    validateCollectionName(inputDto.getEnv(), inputDto.getCollectionName().toString());
  }

  private void validateEnvironment(String env) {
    if (!("dev".equals(env) || "qa".equals(env) || "prod".equals(env))) {
      throw new InputValidatorException("Invalid environment: " + env);
    }
  }

  private void validateCollectionName(String env, String collectionName) {
    AppConfigProperties envConfig = configuration.getConfigForEnv(env);
    if (envConfig == null || !envConfig.getValidCollections().contains(collectionName)) {
      throw new InputValidatorException(
          "Invalid collection name: " + collectionName + " for environment: " + env);
    }
  }
}
