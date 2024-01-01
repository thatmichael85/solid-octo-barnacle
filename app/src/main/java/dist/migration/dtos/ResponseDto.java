package dist.migration.dtos;

import com.amazonaws.services.lambda.runtime.Context;
import com.google.gson.Gson;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ResponseDto {
  private Context awsContext;
  private String dataBaseName;
  private String collectionName;
  private EventType eventType;
  private String result;

  @Override
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
}
