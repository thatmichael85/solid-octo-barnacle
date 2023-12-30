package dist.migration.handlers;

public class MigrationExecutorException extends RuntimeException{
    public MigrationExecutorException(String message) {
        super(message);
    }

    public MigrationExecutorException(String message, Throwable cause) {
        super(message, cause);
    }
}
