package software.amazon.eventschemas.registry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.BadRequestException;
import software.amazon.awssdk.services.schemas.model.ConflictException;
import software.amazon.awssdk.services.schemas.model.CreateRegistryResponse;
import software.amazon.awssdk.services.schemas.model.ForbiddenException;
import software.amazon.awssdk.services.schemas.model.TooManyRequestsException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<SchemasClient> proxyClient;

    @Mock
    SchemasClient schemasClient;

    final CreateHandler handler = new CreateHandler();

    @BeforeEach
    public void setup() {
        // Creates a Mockito wrapper around this object, allowing us to replace the output of the invoke() method.
        proxy = Mockito.spy(new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis()));
        schemasClient = mock(SchemasClient.class);
        proxyClient = MOCK_PROXY(proxy, schemasClient);
    }

    @AfterEach
    public void tear_down() {
        verify(schemasClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(schemasClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        List<TagsEntry> tags = Collections.singletonList(TagsEntry.builder().key("key1").value("value1").build());

        final ResourceModel model = ResourceModel.builder()
                .id("id")
                .registryName("registry")
                .description("simple-description")
                .tags(tags)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        Map<String, String> parsedTag = Collections.singletonMap("key1", "value1");

        // The expected responses
        doReturn(CreateRegistryResponse.builder()
                .registryName("registry")
                .description("simple-description")
                .tags(parsedTag)
                .build())
        .when(proxy)
        .injectCredentialsAndInvokeV2(ArgumentMatchers.any(), ArgumentMatchers.any());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getResourceModel()).isEqualTo(model);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessWithOutIdProvide() {
        final ResourceModel model = ResourceModel.builder()
                .registryName("registry")
                .description("simple-description")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .logicalResourceIdentifier("id")
                .clientRequestToken("token")
                .build();

        // The expected responses
        doReturn(CreateRegistryResponse.builder()
                .registryName("registry")
                .description("simple-description")
                .build())
        .when(proxy)
        .injectCredentialsAndInvokeV2(ArgumentMatchers.any(), ArgumentMatchers.any());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response.getResourceModel()).isEqualTo(model);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_AlreadyExistsFailure() {
        final ResourceModel model = ResourceModel.builder()
                .id("registry")
                .registryName("already-exist-registry")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        doThrow(ConflictException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any()
                );

        assertThrows(CfnAlreadyExistsException.class, () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }

    @Test
    public void handleRequest_ForbiddenFailure() {
        final ResourceModel model = ResourceModel.builder()
                .id("registry")
                .registryName("1p-registry")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        doThrow(ForbiddenException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any()
                );

        assertThrows(CfnAccessDeniedException.class, () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }

    @Test
    public void handleRequest_InternalFailure() {
        final ResourceModel model = ResourceModel.builder()
                .id("registry")
                .registryName("registry")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        doThrow(BadRequestException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any()
                );

        assertThrows(CfnInternalFailureException.class, () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }

    @Test
    public void handleRequest_TooManyRequestFailure() {
        final ResourceModel model = ResourceModel.builder()
                .id("registry")
                .registryName("registry")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        doThrow(TooManyRequestsException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any()
                );

        assertThrows(CfnServiceLimitExceededException.class, () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }

    @Test
    public void handleRequest_OtherFailure() {
        final ResourceModel model = ResourceModel.builder()
                .id("registry")
                .registryName("registry")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        doThrow(AwsServiceException.class)
                .when(proxy)
                .injectCredentialsAndInvokeV2(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any()
                );

        assertThrows(CfnGeneralServiceException.class, () -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));
    }
}
