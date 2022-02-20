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
package io.trino.plugin.exchange.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.trino.plugin.exchange.ExchangeStorageWriter;
import io.trino.plugin.exchange.FileStatus;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import org.openjdk.jol.info.ClassLayout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.plugin.exchange.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.s3.S3RequestUtil.configureEncryption;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3FileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final String DIRECTORY_SUFFIX = "_$folder$";

    private final Optional<Region> region;
    private final Optional<String> endpoint;
    private final int multiUploadPartSize;
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;

    @Inject
    public S3FileSystemExchangeStorage(ExchangeS3Config config)
    {
        requireNonNull(config, "config is null");
        this.region = config.getS3Region();
        this.endpoint = config.getS3Endpoint();
        this.multiUploadPartSize = toIntExact(config.getS3UploadPartSize().toBytes());

        AwsCredentialsProvider credentialsProvider = createAwsCredentialsProvider(config);
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(config.getS3MaxErrorRetries())
                .build();
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(USER_AGENT_PREFIX, "")
                .putAdvancedOption(USER_AGENT_SUFFIX, "Trino-exchange")
                .build();

        this.s3Client = createS3Client(credentialsProvider, overrideConfig);
        this.s3AsyncClient = createS3AsyncClient(credentialsProvider, overrideConfig);
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        // Nothing to do for S3
    }

    @Override
    public SliceInput getSliceInput(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
                .bucket(getBucketName(file))
                .key(keyFromUri(file));
        configureEncryption(secretKey, getObjectRequestBuilder);

        try {
            return new InputStreamSliceInput(s3Client.getObject(getObjectRequestBuilder.build(), ResponseTransformer.toInputStream()));
        }
        catch (RuntimeException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
    {
        String bucketName = getBucketName(file);
        String key = keyFromUri(file);

        return new S3ExchangeStorageWriter(s3AsyncClient, bucketName, key, multiUploadPartSize, secretKey);
    }

    @Override
    public boolean exists(URI file)
            throws IOException
    {
        // Only used for commit marker files and doesn't need secretKey
        return headObject(file, Optional.empty()) != null;
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(getBucketName(file))
                .key(keyFromUri(file))
                .build();

        return transformFuture(toListenableFuture(s3AsyncClient.putObject(request, AsyncRequestBody.empty())));
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(URI uri)
    {
        checkArgument(isDirectory(uri), "deleteRecursively called on file uri");

        ImmutableList.Builder<String> keys = ImmutableList.builder();
        return transformFuture(Futures.transformAsync(
                toListenableFuture((listObjectsRecursively(uri).subscribe(listObjectsV2Response ->
                        listObjectsV2Response.contents().stream().map(S3Object::key).forEach(keys::add)))),
                ignored -> {
                    keys.add(keyFromUri(uri) + DIRECTORY_SUFFIX);
                    return deleteObjects(getBucketName(uri), keys.build());
                },
                directExecutor()));
    }

    @Override
    public List<FileStatus> listFiles(URI dir)
            throws IOException
    {
        ImmutableList.Builder<FileStatus> builder = ImmutableList.builder();
        try {
            for (S3Object object : listObjects(dir).contents()) {
                builder.add(new FileStatus(
                        new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + object.key(), dir.getFragment()).toString(),
                        object.size()));
            }
        }
        catch (RuntimeException e) {
            throw new IOException(e);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return builder.build();
    }

    @Override
    public List<URI> listDirectories(URI dir)
            throws IOException
    {
        ImmutableList.Builder<URI> builder = ImmutableList.builder();
        try {
            for (CommonPrefix prefix : listObjects(dir).commonPrefixes()) {
                builder.add(new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + prefix.prefix(), dir.getFragment()));
            }
        }
        catch (RuntimeException e) {
            throw new IOException(e);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return builder.build();
    }

    @Override
    public int getWriteBufferSize()
    {
        return multiUploadPartSize;
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(s3Client::close);
            closer.register(s3AsyncClient::close);
        }
    }

    private HeadObjectResponse headObject(URI uri, Optional<SecretKey> secretKey)
            throws IOException
    {
        HeadObjectRequest.Builder headObjectRequestBuilder = HeadObjectRequest.builder()
                .bucket(getBucketName(uri))
                .key(keyFromUri(uri));
        configureEncryption(secretKey, headObjectRequestBuilder);

        try {
            return s3Client.headObject(headObjectRequestBuilder.build());
        }
        catch (RuntimeException e) {
            if (e instanceof NoSuchKeyException) {
                return null;
            }
            throw new IOException(e);
        }
    }

    private ListObjectsV2Iterable listObjects(URI dir)
    {
        String key = keyFromUri(dir);
        if (!key.isEmpty()) {
            key += PATH_SEPARATOR;
        }

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(getBucketName(dir))
                .prefix(key)
                .delimiter(PATH_SEPARATOR)
                .build();

        return s3Client.listObjectsV2Paginator(request);
    }

    private ListObjectsV2Publisher listObjectsRecursively(URI dir)
    {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(getBucketName(dir))
                .prefix(keyFromUri(dir))
                .build();

        return s3AsyncClient.listObjectsV2Paginator(request);
    }

    private ListenableFuture<List<DeleteObjectsResponse>> deleteObjects(String bucketName, List<String> keys)
    {
        List<List<String>> subList = Lists.partition(keys, 1000); //  deleteObjects has a limit of 1000
        return Futures.allAsList(subList.stream().map(list -> {
            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(list.stream().map(key -> ObjectIdentifier.builder().key(key).build()).collect(toImmutableList())).build())
                    .build();
            return toListenableFuture(s3AsyncClient.deleteObjects(request));
        }).collect(toImmutableList()));
    }

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    private static String getBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    private static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    // Helper function that translates exception and transform future type to avoid abstraction leak
    private static ListenableFuture<Void> transformFuture(ListenableFuture<?> listenableFuture)
    {
        return asVoid(Futures.catchingAsync(listenableFuture, Throwable.class, throwable -> {
            if (throwable instanceof Error || throwable instanceof IOException) {
                return immediateFailedFuture(throwable);
            }
            else {
                return immediateFailedFuture(new IOException(throwable));
            }
        }, directExecutor()));
    }

    private static boolean isDirectory(URI uri)
    {
        return uri.toString().endsWith(PATH_SEPARATOR);
    }

    private static AwsCredentialsProvider createAwsCredentialsProvider(ExchangeS3Config config)
    {
        if (config.getS3AwsAccessKey() != null && config.getS3AwsSecretKey() != null) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.getS3AwsAccessKey(), config.getS3AwsSecretKey()));
        }
        return DefaultCredentialsProvider.create();
    }

    private S3Client createS3Client(AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration overrideConfig)
    {
        S3ClientBuilder clientBuilder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfig);

        region.ifPresent(clientBuilder::region);
        endpoint.ifPresent(s3Endpoint -> clientBuilder.endpointOverride(URI.create(s3Endpoint)));

        return clientBuilder.build();
    }

    private S3AsyncClient createS3AsyncClient(AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration overrideConfig)
    {
        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfig);

        region.ifPresent(clientBuilder::region);
        endpoint.ifPresent(s3Endpoint -> clientBuilder.endpointOverride(URI.create(s3Endpoint)));

        return clientBuilder.build();
    }

    @NotThreadSafe
    private static class S3ExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(S3ExchangeStorageWriter.class).instanceSize();

        private final S3AsyncClient s3AsyncClient;
        private final String bucketName;
        private final String key;
        private final int partSize;
        private final Optional<SecretKey> secretKey;

        private int currentPartNumber;
        private ListenableFuture<Void> directUploadFuture;
        private ListenableFuture<String> multiPartUploadIdFuture;
        private final List<ListenableFuture<CompletedPart>> multiPartUploadFutures = new ArrayList<>();
        private volatile boolean closed;

        public S3ExchangeStorageWriter(S3AsyncClient s3AsyncClient, String bucketName, String key, int partSize, Optional<SecretKey> secretKey)
        {
            this.s3AsyncClient = requireNonNull(s3AsyncClient, "s3AsyncClient is null");
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.partSize = partSize;
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            checkState(directUploadFuture == null, "Direct upload already started");
            if (closed) {
                // Ignore writes after writer is closed
                return immediateVoidFuture();
            }

            // Skip multipart upload if there would only be one part
            if (slice.length() < partSize && multiPartUploadIdFuture == null) {
                PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key);
                configureEncryption(secretKey, putObjectRequestBuilder);
                directUploadFuture = transformFuture(toListenableFuture(s3AsyncClient.putObject(putObjectRequestBuilder.build(),
                        ByteBufferAsyncRequestBody.fromByteBuffer(slice.toByteBuffer()))));
                return directUploadFuture;
            }

            if (multiPartUploadIdFuture == null) {
                multiPartUploadIdFuture = Futures.transform(createMultipartUpload(), CreateMultipartUploadResponse::uploadId, directExecutor());
            }

            int partNum = ++currentPartNumber;
            ListenableFuture<CompletedPart> uploadFuture = Futures.transformAsync(multiPartUploadIdFuture, uploadId -> uploadPart(uploadId, slice, partNum), directExecutor());
            multiPartUploadFutures.add(uploadFuture);

            return transformFuture(uploadFuture);
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            if (closed) {
                return immediateVoidFuture();
            }

            if (multiPartUploadIdFuture == null) {
                return requireNonNullElseGet(directUploadFuture, Futures::immediateVoidFuture);
            }

            ListenableFuture<Void> finishFuture = transformFuture(Futures.transformAsync(
                    Futures.allAsList(multiPartUploadFutures),
                    completedParts -> completeMultipartUpload(getFutureValue(multiPartUploadIdFuture), completedParts),
                    directExecutor()));
            Futures.addCallback(finishFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(Void result)
                {
                    closed = true;
                }

                @Override
                public void onFailure(Throwable ignored)
                {
                    // Rely on caller to abort in case of exceptions during finish
                }
                }, directExecutor());
            return finishFuture;
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            if (closed) {
                return immediateVoidFuture();
            }
            closed = true;

            if (multiPartUploadIdFuture == null) {
                if (directUploadFuture != null) {
                    directUploadFuture.cancel(true);
                }
                return immediateVoidFuture();
            }

            verify(directUploadFuture == null);
            multiPartUploadFutures.forEach(future -> future.cancel(true));
            return transformFuture(Futures.transformAsync(multiPartUploadIdFuture, this::abortMultipartUpload, directExecutor()));
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }

        private ListenableFuture<CreateMultipartUploadResponse> createMultipartUpload()
        {
            CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key);
            configureEncryption(secretKey, createMultipartUploadRequestBuilder);
            return toListenableFuture(s3AsyncClient.createMultipartUpload(createMultipartUploadRequestBuilder.build()));
        }

        private ListenableFuture<CompletedPart> uploadPart(String uploadId, Slice slice, int partNumber)
        {
            UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber);
            configureEncryption(secretKey, uploadPartRequestBuilder);
            UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();
            return Futures.transform(toListenableFuture(s3AsyncClient.uploadPart(uploadPartRequest, ByteBufferAsyncRequestBody.fromByteBuffer(slice.toByteBuffer()))),
                    uploadPartResponse -> CompletedPart.builder().eTag(uploadPartResponse.eTag()).partNumber(partNumber).build(), directExecutor());
        }

        private ListenableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(String uploadId, List<CompletedPart> completedParts)
        {
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();
            return toListenableFuture(s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest));
        }

        private ListenableFuture<AbortMultipartUploadResponse> abortMultipartUpload(String uploadId)
        {
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .build();
            return toListenableFuture(s3AsyncClient.abortMultipartUpload(abortMultipartUploadRequest));
        }
    }
}
