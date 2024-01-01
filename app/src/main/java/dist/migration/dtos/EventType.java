package dist.migration.dtos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {
    dropCollection,
    checkConnectivity,
    getCollectionSize,
    executeMigration;
}