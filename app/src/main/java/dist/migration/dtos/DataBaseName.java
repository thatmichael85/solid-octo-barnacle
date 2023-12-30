package dist.migration.dtos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DataBaseName {
  DEFAULT_DATA_BASE("DefaultDatabase");

  private String value;

  DataBaseName(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  @JsonCreator
  public static DataBaseName forValue(String value) {
    for (DataBaseName dataBaseName : DataBaseName.values()) {
      if (dataBaseName.getValue().equals(value)) {
        return dataBaseName;
      }
    }
    throw new IllegalArgumentException("Invalid database name: " + value);
  }
}
