package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.services.schemas.SchemasClient;
import java.net.URI;

public class ClientBuilder {

    static SchemasClient getSchemasClient() {
        return SchemasClient.builder().endpointOverride(URI.create("https://schemas-gamma.us-east-1.amazonaws.com")).build();
    }
}
