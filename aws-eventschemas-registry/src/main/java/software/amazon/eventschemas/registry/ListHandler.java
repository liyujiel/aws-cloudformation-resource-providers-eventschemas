package software.amazon.eventschemas.registry;

import software.amazon.awssdk.services.schemas.model.ListRegistriesRequest;
import software.amazon.awssdk.services.schemas.model.ListRegistriesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ListRegistriesRequest awsRequest = Translator.translateToListRequest(request.getNextToken());
        ListRegistriesResponse awsResponse = proxy.injectCredentialsAndInvokeV2(awsRequest, ClientBuilder.getClient()::listRegistries);

        String nextToken = awsResponse.nextToken();
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(Translator.translateFromList(awsResponse))
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
