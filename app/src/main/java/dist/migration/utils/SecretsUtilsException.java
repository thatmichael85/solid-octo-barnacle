package dist.migration.utils;

public class SecretsUtilsException extends RuntimeException {
    public SecretsUtilsException(String message) {
        super(message);
    }

    public SecretsUtilsException(String message, Throwable cause) {
        super(message, cause);
    }
}
