package dist.migration.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import dist.migration.configs.AppConfigProperties;
import dist.migration.configs.Configuration;
import dist.migration.dtos.InputDto;

import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class MonoMigrationHandler implements RequestHandler<InputDto, String> {
  private static Logger log = LoggerFactory.getLogger(MonoMigrationHandler.class);
  private static final String CONFIG_FILE = "/config.yaml";
  private static String arn =
      "arn:aws:secretsmanager:us-east-1:000000000000:secret:test-secret-vozPFj";

  @Override
  public String handleRequest(InputDto input, Context context) {
    Configuration config = loadConfig();
    AppConfigProperties appConfig = config.getConfigForEnv(input.getEnv());
    return appConfig.toString();
  }

  private static Configuration loadConfig() {
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setAllowDuplicateKeys(false);
    Yaml yaml = new Yaml(new Constructor(Configuration.class, loaderOptions));
    try (InputStream in = MonoMigrationHandler.class.getResourceAsStream(MonoMigrationHandler.CONFIG_FILE)) {
      log.info("Loaded configuration successfully");
      return yaml.load(in);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }
}
