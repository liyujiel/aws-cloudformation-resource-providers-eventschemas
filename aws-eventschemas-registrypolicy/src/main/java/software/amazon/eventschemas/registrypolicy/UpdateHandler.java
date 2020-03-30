package software.amazon.eventschemas.registrypolicy;

import software.amazon.awssdk.services.schemas.model.ConflictException;
import software.amazon.awssdk.services.schemas.model.GetPolicyRequest;
import software.amazon.awssdk.services.schemas.model.PutPolicyRequest;
import software.amazon.awssdk.services.schemas.model.PutPolicyResponse;
import software.amazon.awssdk.services.schemas.model.SchemasException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.NotFoundException;

import static software.amazon.eventschemas.registrypolicy.ResourceModel.TYPE_NAME;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    private static final int CALLBACK_DELAY_SECONDS = 30;
    private static final int NUMBER_OF_CREATE_POLL_RETRIES = 3;

    private final SchemasClient schemasClient = ClientBuilder.getSchemasClient();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final CallbackContext context = callbackContext == null ? CallbackContext.builder()
                .registryPolicyUpdated(false)
                .registryPolicyStabilized(false)
                .stabilizationRetriesRemaining(NUMBER_OF_CREATE_POLL_RETRIES)
                .build() : callbackContext;

        final ResourceModel resourceModel = request.getDesiredResourceState();
        final String registryName = resourceModel.getRegistryName();

        if (resourceModel.getId() == null) {
            resourceModel.setId(registryName);
        }

        if (!context.isRegistryPolicyUpdated()) {
            String revisionId = getCurrentRevisionId(registryName, proxy);
            PutPolicyResponse putPolicyResponse = updatePolicy(registryName, revisionId, resourceModel.getPolicy(), proxy);

            context.setRegistryPolicyUpdated(true);
            resourceModel.setRevisionId(putPolicyResponse.revisionId());

            logger.log(String.format("%s [%s] updated successfully",
                    ResourceModel.TYPE_NAME, registryName));
        }

        if (!context.isRegistryPolicyStabilized()) {
            boolean stabilized = isRegistryPolicyStabilized(registryName, resourceModel.getRevisionId(), proxy);
            if (!stabilized) {
                context.decrementStabilizationRetriesRemaining();
            }
            context.setRegistryPolicyStabilized(stabilized);
        }

        if (!context.isRegistryPolicyStabilized()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .callbackContext(context)
                    .status(OperationStatus.IN_PROGRESS)
                    .callbackDelaySeconds(CALLBACK_DELAY_SECONDS)
                    .resourceModel(resourceModel)
                    .build();
        }

        return ProgressEvent.defaultSuccessHandler(resourceModel);
    }

    private boolean isRegistryPolicyStabilized(String registryName, String revisionId, AmazonWebServicesClientProxy proxy) {
        try {
            GetPolicyRequest getPolicyRequest = GetPolicyRequest.builder().registryName(registryName).build();
            String revisionReturned = proxy.injectCredentialsAndInvokeV2(getPolicyRequest, schemasClient::getPolicy).revisionId();
            return revisionReturned.equals(revisionId);
        } catch (NotFoundException e) {
            return false;
        } catch (SchemasException e) {
            throw new CfnGeneralServiceException("UpdateRegistryPolicy", e);
        }
    }

    private String getCurrentRevisionId(String registryName, AmazonWebServicesClientProxy proxy) {
        try {
            GetPolicyRequest getPolicyRequest = GetPolicyRequest.builder().registryName(registryName).build();
            return proxy.injectCredentialsAndInvokeV2(getPolicyRequest, schemasClient::getPolicy).revisionId();
        } catch (NotFoundException e) {
            // Either Registry or Policy does not exist
            throw new CfnNotFoundException(TYPE_NAME, registryName, e);
        } catch (SchemasException e) {
            throw new CfnGeneralServiceException("UpdateRegistryPolicy", e);
        }
    }

    private PutPolicyResponse updatePolicy(String registryName, String revisionId, String policy, AmazonWebServicesClientProxy proxy) {
        try {
            PutPolicyRequest putPolicyRequest = PutPolicyRequest.builder().registryName(registryName).policy(policy).revisionId(revisionId).build();
            return proxy.injectCredentialsAndInvokeV2(putPolicyRequest, schemasClient::putPolicy);
        } catch (ConflictException e) {
            throw new CfnResourceConflictException(TYPE_NAME, registryName, e.getMessage());
        } catch (SchemasException e) {
            throw new CfnGeneralServiceException("UpdateRegistryPolicy", e);
        }
    }

}
