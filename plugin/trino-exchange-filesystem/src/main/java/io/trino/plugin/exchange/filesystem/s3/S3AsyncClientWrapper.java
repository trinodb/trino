/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.exchange.filesystem.s3;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.util.VersionInfo;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.ABORT_MULTIPART_UPLOAD;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.COMPLETE_MULTIPART_UPLOAD;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.CREATE_MULTIPART_UPLOAD;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.DELETE_OBJECTS;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.GET_OBJECT;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.LIST_OBJECTS_V2;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.PUT_OBJECT;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.UPLOAD_PART;
import static java.util.Objects.requireNonNull;

public abstract class S3AsyncClientWrapper
        implements S3AsyncClient
{
    private final S3AsyncClient delegate;

    public S3AsyncClientWrapper(S3AsyncClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public String serviceName()
    {
        return delegate.serviceName();
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(PutObjectRequest request, AsyncRequestBody body)
    {
        CompletableFuture<PutObjectResponse> future = delegate.putObject(request, body);
        handle(PUT_OBJECT, future);
        return future;
    }

    @Override
    public CompletableFuture<DeleteObjectsResponse> deleteObjects(DeleteObjectsRequest request)
    {
        CompletableFuture<DeleteObjectsResponse> future = delegate.deleteObjects(request);
        handle(DELETE_OBJECTS, future);
        return future;
    }

    @Override
    public <T> CompletableFuture<T> getObject(GetObjectRequest request, AsyncResponseTransformer<GetObjectResponse, T> transformer)
    {
        CompletableFuture<T> future = delegate.getObject(request, transformer);
        handle(GET_OBJECT, future);
        return future;
    }

    @Override
    public CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload(CreateMultipartUploadRequest request)
    {
        CompletableFuture<CreateMultipartUploadResponse> future = delegate.createMultipartUpload(request);
        handle(CREATE_MULTIPART_UPLOAD, future);
        return future;
    }

    @Override
    public CompletableFuture<UploadPartResponse> uploadPart(UploadPartRequest request, AsyncRequestBody body)
    {
        CompletableFuture<UploadPartResponse> future = delegate.uploadPart(request, body);
        handle(UPLOAD_PART, future);
        return future;
    }

    @Override
    public CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(CompleteMultipartUploadRequest request)
    {
        CompletableFuture<CompleteMultipartUploadResponse> future = delegate.completeMultipartUpload(request);
        handle(COMPLETE_MULTIPART_UPLOAD, future);
        return future;
    }

    @Override
    public CompletableFuture<AbortMultipartUploadResponse> abortMultipartUpload(AbortMultipartUploadRequest request)
    {
        CompletableFuture<AbortMultipartUploadResponse> future = delegate.abortMultipartUpload(request);
        handle(ABORT_MULTIPART_UPLOAD, future);
        return future;
    }

    @Override
    public CompletableFuture<ListObjectsV2Response> listObjectsV2(ListObjectsV2Request request)
    {
        CompletableFuture<ListObjectsV2Response> future = delegate.listObjectsV2(request);
        handle(LIST_OBJECTS_V2, future);
        return future;
    }

    protected abstract void handle(RequestType requestType, CompletableFuture<?> responseFuture);

    @Override
    public ListObjectsV2Publisher listObjectsV2Paginator(ListObjectsV2Request listObjectsV2Request)
    {
        return new ListObjectsV2Publisher(this, applyPaginatorUserAgent(listObjectsV2Request));
    }

    /**
     * Based on {@link software.amazon.awssdk.services.s3.DefaultS3AsyncClient#applyPaginatorUserAgent(S3Request)}
     */
    private <T extends S3Request> T applyPaginatorUserAgent(T request)
    {
        Consumer<AwsRequestOverrideConfiguration.Builder> userAgentApplier = b -> b.addApiName(ApiName.builder()
                .version(VersionInfo.SDK_VERSION).name("PAGINATED").build());
        AwsRequestOverrideConfiguration overrideConfiguration = request.overrideConfiguration()
                .map(c -> c.toBuilder().applyMutation(userAgentApplier).build())
                .orElseGet(() -> AwsRequestOverrideConfiguration.builder().applyMutation(userAgentApplier).build());
        return (T) request.toBuilder().overrideConfiguration(overrideConfiguration).build();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    public enum RequestType
    {
        PUT_OBJECT,
        DELETE_OBJECTS,
        GET_OBJECT,
        CREATE_MULTIPART_UPLOAD,
        UPLOAD_PART,
        COMPLETE_MULTIPART_UPLOAD,
        ABORT_MULTIPART_UPLOAD,
        LIST_OBJECTS_V2,
    }
}
