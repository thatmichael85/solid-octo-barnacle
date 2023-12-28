package dist.migration.dtos;

import lombok.Data;

@Data
public class InputDto {
  private String env;
  private CollectionName collectionName;
  private boolean dropTheCollection;
  private boolean dryRun;
}
