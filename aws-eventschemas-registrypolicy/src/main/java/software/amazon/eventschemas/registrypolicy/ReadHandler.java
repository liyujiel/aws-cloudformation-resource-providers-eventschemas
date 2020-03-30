package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.services.schemas.model.GetPolicyRequest;
import software.amazon.awssdk.services.schemas.model.GetPolicyResponse;
import software.amazon.awssdk.services.schemas.model.SchemasException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.NotFoundException;

import static software.amazon.eventschemas.registrypolicy.ResourceModel.TYPE_NAME;

public class ReadHandler extends BaseHandler<CallbackContext> {

    private final SchemasClient schemasClient = ClientBuilder.getSchemasClient();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel resourceModel = request.getDesiredResourceState();
        final String registryName = resourceModel.getId();

        GetPolicyRequest getPolicyRequest = GetPolicyRequest.builder().registryName(registryName).build();

        try {
            GetPolicyResponse getPolicyResponse = proxy.injectCredentialsAndInvokeV2(getPolicyRequest, schemasClient::getPolicy);

            resourceModel.setPolicy(getPolicyResponse.policy());
            resourceModel.setRevisionId(getPolicyResponse.revisionId());
        } catch (NotFoundException e) {
            throw new CfnNotFoundException(TYPE_NAME, registryName, e);
        } catch (SchemasException e) {
            throw new CfnGeneralServiceException("GetPolicy", e);
        }

        return ProgressEvent.defaultSuccessHandler(resourceModel);
    }

}
