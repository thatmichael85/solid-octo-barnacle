package dist.migration.dtos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {
    DROP_COLLECTION("dropCollection"),
    CHECK_CONNECTIVITY("checkConnectivity"),
    GET_COLLECTION_SIZE("getCollectionSize"),
    EXECUTE_MIGRATION("executeMigration");

    private String value;

    EventType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static EventType forValue(String value) {
        for (EventType eventType : EventType.values()) {
            if (eventType.getValue().equals(value)) {
                return eventType;
            }
        }
        throw new IllegalArgumentException("Invalid event type: " + value);
    }
}