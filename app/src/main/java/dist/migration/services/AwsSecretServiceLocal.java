package dist.migration.services;

import dist.migration.configs.AppConfigProperties;

public class AwsSecretServiceLocal implements AwsSecretsService {

    @Override
    public String getSecret(String secretArn) {
        return secretArn;
    }
}
