package software.amazon.eventschemas.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.BadRequestException;
import software.amazon.awssdk.services.schemas.model.ForbiddenException;
import software.amazon.awssdk.services.schemas.model.NotFoundException;
import software.amazon.awssdk.services.schemas.model.UpdateRegistryRequest;
import software.amazon.awssdk.services.schemas.model.UpdateRegistryResponse;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<SchemasClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        ResourceModel model = request.getDesiredResourceState();

        return ProgressEvent.progress(model, callbackContext)
            .then(progress ->
                proxy.initiate("AWS-EventSchemas-Registry::Update", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToUpdateRequest)
                    .makeServiceCall(this::updateResource)
                    .progress())
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private UpdateRegistryResponse updateResource(UpdateRegistryRequest awsRequest, ProxyClient<SchemasClient> proxyClient) {
        UpdateRegistryResponse awsResponse;
        try {
            awsResponse = proxyClient.injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::updateRegistry);
        } catch (final NotFoundException e) {
            // update resource does not exist
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, awsRequest.registryName(), e);
        } catch (final ForbiddenException e) {
            // customer cannot update 1P
            throw new CfnAccessDeniedException(ResourceModel.TYPE_NAME, e);
        } catch (final BadRequestException e) {
            // internal error
            throw new CfnInternalFailureException(e);
        } catch (final AwsServiceException e) {
            // general exception
            throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
        }

        logger.log(String.format("%s has successfully been updated.", ResourceModel.TYPE_NAME));
        return awsResponse;
    }
}
