package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.services.schemas.model.DeletePolicyRequest;
import software.amazon.awssdk.services.schemas.model.DeletePolicyResponse;
import software.amazon.awssdk.services.schemas.model.GetPolicyRequest;
import software.amazon.awssdk.services.schemas.model.GetPolicyResponse;
import software.amazon.awssdk.services.schemas.model.NotFoundException;
import software.amazon.awssdk.services.schemas.model.SchemasException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
        logger = mock(Logger.class);
    }


    @Test
    public void testSuccessState() {
        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder()
                .registryName("test-registry")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        //Mock
        doReturn(DeletePolicyResponse.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(DeletePolicyRequest.class), any());
        doThrow(NotFoundException.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(GetPolicyRequest.class), any());

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testInProgressState() {
        //GIVEN
        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder()
                .registryName("test-registry")
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();
        final CallbackContext outputContext = CallbackContext.builder()
                .registryPolicyDeleted(true)
                .registryPolicyStabilized(false)
                .stabilizationRetriesRemaining(2)
                .build();

        // Mock
        doReturn(DeletePolicyResponse.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(DeletePolicyRequest.class), any());
        doReturn(GetPolicyResponse.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(GetPolicyRequest.class), any());

        //WHEN
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, null, logger);

        //THEN
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualTo(outputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testResourceNotFoundException() {
        //GIVEN
        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder()
                .registryName("test-registry")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        // Mock
        doThrow(NotFoundException.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(DeletePolicyRequest.class), any());

        //WHEN
        assertThrows(CfnNotFoundException.class, () ->
                handler.handleRequest(proxy, request, null, logger));
    }

    @Test
    public void testSchemasException() {
        //GIVEN
        final DeleteHandler handler = new DeleteHandler();
        final ResourceModel model = ResourceModel.builder()
                .registryName("test-registry")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        // Mock
        doThrow(SchemasException.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(DeletePolicyRequest.class), any());

        //WHEN
        assertThrows(CfnGeneralServiceException.class, () ->
                handler.handleRequest(proxy, request, null, logger));
    }

}
