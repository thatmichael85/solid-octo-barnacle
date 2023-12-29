package dist.migration.validators;

public class InputValidatorException extends RuntimeException {
    public InputValidatorException(String message) {
        super(message);
    }

    public InputValidatorException(String message, Throwable cause) {
        super(message, cause);
    }
}
