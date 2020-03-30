package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

import java.net.URI;

public class ClientBuilder {

    static SchemasClient getSchemasClient() {
        return SchemasClient.builder().endpointOverride(URI.create("https://t4nakn26yh.execute-api.us-west-2.amazonaws.com/api")).region(Region.of("us-west-2")).build();
    }

    static CloudFormationClient getCfnClient() {
        return CloudFormationClient.create();
    }
}
