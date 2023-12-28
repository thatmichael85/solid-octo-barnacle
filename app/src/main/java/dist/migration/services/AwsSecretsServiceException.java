package dist.migration.services;

public class AwsSecretsServiceException extends RuntimeException {
    public AwsSecretsServiceException(String message) {
        super(message);
    }

    public AwsSecretsServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
