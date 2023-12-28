package dist.migration.configs;


import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class Configuration {
    private Map<String, AppConfigProperties> environments;
    public AppConfigProperties getConfigForEnv(String env) {
        if (!environments.containsKey(env)) {
            throw new IllegalArgumentException("Invalid environment: " + env);
        }
        return environments.get(env);
    }
}