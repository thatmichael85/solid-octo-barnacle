package dist.migration.dtos;

import lombok.Data;

@Data
public class InputDto {
  private String env;
  private String dataBaseName;
  private String collectionName;
  private EventType eventType;
}
