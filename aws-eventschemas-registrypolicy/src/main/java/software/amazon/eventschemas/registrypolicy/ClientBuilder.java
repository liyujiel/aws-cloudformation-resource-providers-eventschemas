package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.schemas.SchemasClient;
import java.net.URI;

public class ClientBuilder {

    static SchemasClient getSchemasClient() {
        final String preprodServiceName = "schemas-preprod";
        return SchemasClient.builder()
                .endpointOverride(URI.create("https://schemas-gamma.us-east-1.amazonaws.com"))
                .overrideConfiguration(
                        ClientOverrideConfiguration.builder()
                                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, new SchemasSigner(preprodServiceName))
                                .build())
                .build();
    }
}
