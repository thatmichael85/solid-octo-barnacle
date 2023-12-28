package dist.migration.utils;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class SecretsUtil {

  private static final Logger logger = LoggerFactory.getLogger(SecretsUtil.class);
  private static final String AWS_REGION_US_EAST_1 = "us-east-1";

  public static String getSecret(String secretArn) {
    try (SecretsManagerClient client =
        SecretsManagerClient.builder()
            .endpointOverride(URI.create("http://localstack:4566"))
            .region(Region.of(AWS_REGION_US_EAST_1)) // replace with your AWS region
            .build()) {

      logger.info("Retrieving secret ARN: {}", secretArn);
      GetSecretValueRequest valueRequest =
          GetSecretValueRequest.builder().secretId(secretArn).build();

      GetSecretValueResponse valueResponse = client.getSecretValue(valueRequest);
      logger.info(
          "Successfully retrieved secret ARN: {},{}", secretArn, valueResponse.secretString());
      return valueResponse.secretString();
    } catch (Exception e) {
      throw new SecretsUtilsException("Failed to retrieve secret from AWS Secrets Manager", e);
    }
  }
}
