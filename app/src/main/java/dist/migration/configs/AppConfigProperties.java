package dist.migration.configs;

import lombok.Data;

@Data
public class AppConfigProperties {
    private String sourceUrl;
    private String sourceDatabase;
    private String sourceUserNameArn;
    private String sourceUserPasswordArn;

    private String destinationUrl;
    private String destinationDatabase;
    private String destinationUserNameArn;
    private String destinationUserPasswordArn;
}
