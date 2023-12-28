package dist.migration.services;

public class MongoMigrationServiceException extends RuntimeException {
    public MongoMigrationServiceException(String message) {
        super(message);
    }

    public MongoMigrationServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
