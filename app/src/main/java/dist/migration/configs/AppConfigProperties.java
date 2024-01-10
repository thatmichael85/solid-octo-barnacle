package dist.migration.configs;

import java.util.Map;
import lombok.Data;

@Data
public class AppConfigProperties {
  private Map<String, DatabaseProperties> databases;
}