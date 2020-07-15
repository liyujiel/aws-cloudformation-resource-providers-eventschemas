package software.amazon.eventschemas.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.BadRequestException;
import software.amazon.awssdk.services.schemas.model.DeleteRegistryRequest;
import software.amazon.awssdk.services.schemas.model.DeleteRegistryResponse;
import software.amazon.awssdk.services.schemas.model.ForbiddenException;
import software.amazon.awssdk.services.schemas.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<SchemasClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        final ResourceModel resourceModel = request.getDesiredResourceState();

        return ProgressEvent.progress(resourceModel, callbackContext)
            .then(progress ->
                proxy.initiate("AWS-EventSchemas-Registry::Delete", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToDeleteRequest)
                    .makeServiceCall(this::deleteResource)
                    .success());
    }

    private DeleteRegistryResponse deleteResource(DeleteRegistryRequest awsRequest, ProxyClient<SchemasClient> client) {
        {
            DeleteRegistryResponse awsResponse;
            try {
                awsResponse = client.injectCredentialsAndInvokeV2(awsRequest, client.client()::deleteRegistry);
            } catch (final NotFoundException e) {
                // delete resource does not exist
                throw new CfnNotFoundException(ResourceModel.TYPE_NAME, awsRequest.registryName(), e);
            } catch (final ForbiddenException e) {
                // customer cannot delete 1P
                throw new CfnAccessDeniedException(ResourceModel.TYPE_NAME, e);
            } catch (final BadRequestException e) {
                // internal error
                throw new CfnInternalFailureException(e);
            } catch (final AwsServiceException e) {
                // general exception
                throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
            }

            logger.log(String.format("%s successfully deleted.", ResourceModel.TYPE_NAME));
            return awsResponse;
        }
    }
}
