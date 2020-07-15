package software.amazon.eventschemas.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.BadRequestException;
import software.amazon.awssdk.services.schemas.model.ConflictException;
import software.amazon.awssdk.services.schemas.model.CreateRegistryRequest;
import software.amazon.awssdk.services.schemas.model.CreateRegistryResponse;
import software.amazon.awssdk.services.schemas.model.ForbiddenException;
import software.amazon.awssdk.services.schemas.model.TooManyRequestsException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;


public class CreateHandler extends BaseHandlerStd {

    private static final int REGISTRY_NAME_LENGTH = 50;

    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<SchemasClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        ResourceModel resourceModel = request.getDesiredResourceState();

        if (resourceModel.getId() == null) {
            resourceModel.setId(
                    IdentifierUtils
                            .generateResourceIdentifier(request.getLogicalResourceIdentifier(),
                                    request.getClientRequestToken(),
                                    REGISTRY_NAME_LENGTH)
                            .toLowerCase()
            );
        }

        return ProgressEvent.progress(resourceModel, callbackContext)
            .then(progress ->
                proxy.initiate("AWS-EventSchemas-Registry::Create", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                    .translateToServiceRequest(Translator::translateToCreateRequest)
                    .makeServiceCall(this::createResource)
                    .done(this::constructResourceModelFromResponse)
                );
    }

    private ProgressEvent<ResourceModel, CallbackContext> constructResourceModelFromResponse(CreateRegistryRequest createRegistryRequest, CreateRegistryResponse response, ProxyClient<SchemasClient> schemasClientProxyClient, ResourceModel resourceModel, CallbackContext callbackContext) {
        return ProgressEvent.defaultSuccessHandler(Translator.translateToCreateResponse(resourceModel, response));
    }

    private CreateRegistryResponse createResource(CreateRegistryRequest awsRequest, ProxyClient<SchemasClient> proxyClient) {
        CreateRegistryResponse awsResponse;
        try {
            awsResponse = proxyClient.injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::createRegistry);
        } catch (final ConflictException e) {
            // create resource already exist
            throw new CfnAlreadyExistsException(ResourceModel.TYPE_NAME, awsRequest.registryName());
        } catch (final ForbiddenException e) {
            // customer cannot create 1p registry
            throw new CfnAccessDeniedException(e);
        } catch (final TooManyRequestsException e) {
            // throttle or resource limit exceed
            throw new CfnServiceLimitExceededException(ResourceModel.TYPE_NAME, awsRequest.registryName(), e);
        }  catch (final BadRequestException e) {
            // internal error
            throw new CfnInternalFailureException(e);
        } catch (final AwsServiceException e) {
            // general exception
            throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
        }

        logger.log(String.format("%s successfully created.", ResourceModel.TYPE_NAME));
        return awsResponse;
    }
}
