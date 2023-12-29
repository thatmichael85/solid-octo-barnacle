package dist.migration.services;

public interface AwsSecretsService {
    public String getSecret(String secretArn);
}
