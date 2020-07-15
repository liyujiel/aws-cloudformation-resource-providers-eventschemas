package software.amazon.eventschemas.registry;

import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {

  private ClientBuilder() {}

  public static SchemasClient getClient() {
    return SchemasClient.builder()
            .httpClient(LambdaWrapper.HTTP_CLIENT)
            .build();
  }
}
