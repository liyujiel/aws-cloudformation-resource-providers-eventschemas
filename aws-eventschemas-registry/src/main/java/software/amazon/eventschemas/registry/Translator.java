package software.amazon.eventschemas.registry;

import software.amazon.awssdk.services.schemas.model.CreateRegistryRequest;
import software.amazon.awssdk.services.schemas.model.CreateRegistryResponse;
import software.amazon.awssdk.services.schemas.model.DeleteRegistryRequest;
import software.amazon.awssdk.services.schemas.model.DescribeRegistryRequest;
import software.amazon.awssdk.services.schemas.model.DescribeRegistryResponse;
import software.amazon.awssdk.services.schemas.model.ListRegistriesRequest;
import software.amazon.awssdk.services.schemas.model.ListRegistriesResponse;
import software.amazon.awssdk.services.schemas.model.UpdateRegistryRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

  /**
   * Request to create a resource
   * @param model resource model
   * @return CreateRegistryRequest the registry creation request to create a resource
   */
  static CreateRegistryRequest translateToCreateRequest(final ResourceModel model) {
    return CreateRegistryRequest.builder()
            .registryName(model.getRegistryName())
            .description(model.getDescription())
            .build();
  }

  /**
   * Request to create a resource
   *
   * @param model resource model
   * @param response response
   * @return CreateRegistryRequest the registry creation request to create a resource
   */
  static ResourceModel translateToCreateResponse(final ResourceModel model, final CreateRegistryResponse response) {
    return ResourceModel.builder()
            .id(model.getId())
            .registryName(response.registryName())
            .registryArn(response.registryArn())
            .description(response.description())
            .tags(translateToResourceModelTags(response.tags()))
            .build();
  }

  /**
   * Request to read a resource
   * @param model resource model
   * @return awsRequest the aws service request to describe a resource
   */
  static DescribeRegistryRequest translateToReadRequest(final ResourceModel model) {
    return DescribeRegistryRequest.builder()
            .registryName(model.getRegistryName())
            .build();
  }

  /**
   * Translates resource object from sdk into a resource model
   *
   * @param model resource model
   * @param awsResponse the aws service describe resource response
   * @return model resource model
   */
  static ResourceModel translateFromReadResponse(final ResourceModel model, final DescribeRegistryResponse awsResponse) {
    return ResourceModel.builder()
            .id(model.getId())
            .registryName(awsResponse.registryName())
            .registryArn(awsResponse.registryArn())
            .description(awsResponse.description())
            .build();

  }

  /**
   * Request to delete a resource
   * @param model resource model
   * @return awsRequest the aws service request to delete a resource
   */
  static DeleteRegistryRequest translateToDeleteRequest(final ResourceModel model) {
    return DeleteRegistryRequest.builder()
            .registryName(model.getRegistryName())
            .build();
  }

  /**
   * Request to update properties of a previously created resource
   * @param model resource model
   * @return awsRequest the aws service request to modify a resource
   */
  static UpdateRegistryRequest translateToUpdateRequest(final ResourceModel model) {
    return UpdateRegistryRequest.builder()
            .registryName(model.getRegistryName())
            .description(model.getDescription())
            .build();
  }

  /**
   * Request to list resources
   * @param nextToken token passed to the aws service list resources request
   * @return awsRequest the aws service request to list resources within aws account
   */
  static ListRegistriesRequest translateToListRequest(final String nextToken) {
    return ListRegistriesRequest.builder()
            .limit(50)
            .nextToken(nextToken)
            .build();
  }

  /**
   * Translates resource objects from sdk into a resource model (primary identifier only)
   * @param awsResponse the aws service describe resource response
   * @return list of resource models
   */
  static List<ResourceModel> translateFromList(final ListRegistriesResponse awsResponse) {
    return streamOfOrEmpty(awsResponse.registries())
            .map(resource -> ResourceModel.builder()
                .registryName(resource.registryName())
                .registryArn(resource.registryArn())
                .build())
            .collect(Collectors.toList());
  }

  /**
   * Translates Request Tags to Resource Model TagsEntry
   */
  private static List<TagsEntry> translateToResourceModelTags(Map<String, String> tagsMap) {
    // We return null to ensure that the resource model returned by the read request is exactly the same as the one used
    // by the create request. MediaPackageVod creates an empty map by default, but ResourceModel leaves it as null.
    if (tagsMap != null && !tagsMap.isEmpty()) {
      List<TagsEntry> tagList = new ArrayList<>();
      for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
        tagList.add(TagsEntry.builder().key(entry.getKey()).value(entry.getValue()).build());
      }
      return tagList;
    }
    return null;
  }

  private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
    return Optional.ofNullable(collection)
            .map(Collection::stream)
            .orElseGet(Stream::empty);
  }
}
