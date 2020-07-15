package software.amazon.eventschemas.registry;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.schemas.SchemasClient;
import software.amazon.awssdk.services.schemas.model.BadRequestException;
import software.amazon.awssdk.services.schemas.model.DescribeRegistryRequest;
import software.amazon.awssdk.services.schemas.model.DescribeRegistryResponse;
import software.amazon.awssdk.services.schemas.model.NotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInternalFailureException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<SchemasClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate("AWS-EventSchemas-Registry::Read", proxyClient, request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(Translator::translateToReadRequest)
            .makeServiceCall(this::readResource)
            .done(this::constructReadRegistryResponse);
    }

    private ProgressEvent<ResourceModel, CallbackContext> constructReadRegistryResponse(DescribeRegistryRequest describeRegistryRequest, DescribeRegistryResponse awsResponse, ProxyClient<SchemasClient> schemasClientProxyClient, ResourceModel resourceModel, CallbackContext callbackContext) {
        return ProgressEvent.defaultSuccessHandler(Translator.translateFromReadResponse(resourceModel, awsResponse));
    }


    private DescribeRegistryResponse readResource(DescribeRegistryRequest awsRequest, ProxyClient<SchemasClient> proxyClient) {
        DescribeRegistryResponse awsResponse;
        try {
            awsResponse = proxyClient.injectCredentialsAndInvokeV2(awsRequest, proxyClient.client()::describeRegistry);
        } catch (final NotFoundException e) {
            // read resource does not exist
            throw new CfnNotFoundException(ResourceModel.TYPE_NAME, awsRequest.registryName(), e);
        } catch (final BadRequestException e) {
            // internal error
            throw new CfnInternalFailureException(e);
        } catch (final AwsServiceException e) {
            // general exception
            throw new CfnGeneralServiceException(ResourceModel.TYPE_NAME, e);
        }

        logger.log(String.format("%s has successfully been read.", ResourceModel.TYPE_NAME));
        return awsResponse;
    }
}
