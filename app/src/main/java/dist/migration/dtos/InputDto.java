package dist.migration.dtos;

import lombok.Data;

@Data
public class InputDto {
  private String env;
  private DataBaseName dataBaseName;
  private CollectionName collectionName;
  private EventType eventType;
}
