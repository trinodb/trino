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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.trino.plugin.exchange.ExchangeStorageWriter;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.util.Mimetype;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import javax.annotation.PreDestroy;
import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.plugin.exchange.FileSystemExchangeManager.PATH_SEPARATOR;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;
import static software.amazon.awssdk.core.sync.RequestBody.fromContentProvider;

public class S3FileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final String DIRECTORY_SUFFIX = "_$folder$";

    private final Region region;
    private final int multiUploadPartSize;
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;

    private final ExecutorService deleteExecutor;

    @Inject
    public S3FileSystemExchangeStorage(ExchangeS3Config config)
    {
        if (config.getS3Region() != null) {
            this.region = Region.of(config.getS3Region().toLowerCase(ENGLISH));
        }
        else {
            this.region = null;
        }
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
        this.deleteExecutor = Executors.newFixedThreadPool(
                config.getDeletionThreadCount(),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("exchange-s3-deletion-%d")
                        .build());
    }

    @Override
    public void initialize(URI baseDirectory)
    {
        // TODO: decide if we want to check for expiration life cycle rules
        String bucketName = getBucketName(baseDirectory);
        GetBucketLifecycleConfigurationRequest request = GetBucketLifecycleConfigurationRequest.builder()
                .bucket(bucketName)
                .build();
        GetBucketLifecycleConfigurationResponse response = s3Client.getBucketLifecycleConfiguration(request);

        verify(response.rules().stream().anyMatch(
                rule -> rule.expiration() != null &&
                        rule.abortIncompleteMultipartUpload() != null &&
                        rule.status().equals(ExpirationStatus.ENABLED) &&
                        rule.filter().equals(LifecycleRuleFilter.builder().build())
                ), "Expected file expiration and abortIncompleteMultipartUpload lifecycle rule for exchange bucket %s", baseDirectory.toString());
    }

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        // no need to do anything for S3
    }

    @Override
    public SliceInput getSliceInput(URI file, Optional<SecretKey> secretKey)
            throws IOException
    {
        GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
                .bucket(getBucketName(file))
                .key(keyFromUri(file));
        S3RequestUtil.configureEncryption(secretKey, getObjectRequestBuilder);

        try {
            return new InputStreamSliceInput(s3Client.getObject(getObjectRequestBuilder.build(), ResponseTransformer.toInputStream()));
        }
        catch (AwsServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
    {
        String bucketName = getBucketName(file);
        String key = keyFromUri(file);

        return new S3ExchangeStorageWriter(s3Client, s3AsyncClient, bucketName, key, multiUploadPartSize, secretKey);
    }

    @Override
    public boolean exists(URI file)
            throws IOException
    {
        // Only used for commit marker files and doesn't need secretKey
        return headObject(file, Optional.empty()) != null;
    }

    @Override
    public void createEmptyFile(URI file)
            throws IOException
    {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(getBucketName(file))
                .key(keyFromUri(file))
                .build();

        try {
            s3Client.putObject(request, RequestBody.empty());
        }
        catch (AwsServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteRecursively(URI uri)
    {
        deleteExecutor.submit(() -> {
            if (isDirectory(uri)) {
                ImmutableList.Builder<String> keys = ImmutableList.builder();
                for (S3Object s3Object : listObjectsRecursively(uri).contents()) {
                    keys.add(s3Object.key());
                }
                keys.add(keyFromUri(uri) + DIRECTORY_SUFFIX);

                deleteObjects(getBucketName(uri), keys.build());
            }
            else {
                deleteObject(getBucketName(uri), keyFromUri(uri));
            }
        });
    }

    @Override
    public Stream<URI> listFiles(URI dir)
    {
        return listObjects(dir).contents().stream().filter(object -> !object.key().endsWith(PATH_SEPARATOR)).map(object -> {
            try {
                return new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + object.key(), dir.getFragment());
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    @Override
    public Stream<URI> listDirectories(URI dir)
    {
        return listObjects(dir).commonPrefixes().stream().map(prefix -> {
            try {
                return new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + prefix.prefix(), dir.getFragment());
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        });
    }

    @Override
    public long size(URI uri, Optional<SecretKey> secretKey)
            throws IOException
    {
        checkArgument(!isDirectory(uri), "expected a file URI but got a directory URI");
        HeadObjectResponse response = headObject(uri, secretKey);
        if (response == null) {
            throw new FileNotFoundException("File does not exist: " + uri);
        }
        return response.contentLength();
    }

    @Override
    public int getWriteBufferSizeInBytes()
    {
        return multiUploadPartSize;
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(deleteExecutor::shutdown);
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
        S3RequestUtil.configureEncryption(secretKey, headObjectRequestBuilder);

        try {
            return s3Client.headObject(headObjectRequestBuilder.build());
        }
        catch (AwsServiceException e) {
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

    private ListObjectsV2Iterable listObjectsRecursively(URI dir)
    {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(getBucketName(dir))
                .prefix(keyFromUri(dir))
                .build();

        return s3Client.listObjectsV2Paginator(request);
    }

    private void deleteObject(String bucketName, String key)
    {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        s3Client.deleteObject(request);
    }

    private void deleteObjects(String bucketName, List<String> keys)
    {
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(Delete.builder().objects(keys.stream().map(s -> ObjectIdentifier.builder().key(s).build()).collect(toImmutableList())).build())
                .build();
        s3Client.deleteObjects(request);
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

        if (region != null) {
            clientBuilder = clientBuilder.region(region);
        }

        return clientBuilder.build();
    }

    private S3AsyncClient createS3AsyncClient(AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration overrideConfig)
    {
        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfig);

        if (region != null) {
            clientBuilder = clientBuilder.region(region);
        }

        return clientBuilder.build();
    }

    private static class S3ExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private final S3Client s3Client;
        private final S3AsyncClient s3AsyncClient;
        private final String bucketName;
        private final String key;
        private final int partSize;
        private final Optional<SecretKey> secretKey;

        private int currentPartNumber;
        private Optional<String> uploadId = Optional.empty();
        private final List<CompletableFuture<CompletedPart>> uploadFutures = new ArrayList<>();

        public S3ExchangeStorageWriter(S3Client s3Client, S3AsyncClient s3AsyncClient, String bucketName, String key, int partSize, Optional<SecretKey> secretKey)
        {
            this.s3Client = requireNonNull(s3Client, "s3Client is null");
            this.s3AsyncClient = requireNonNull(s3AsyncClient, "s3AsyncClient is null");
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.partSize = partSize;
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
                throws IOException
        {
            // skip multipart upload if there would only be one part
            if (slice.length() < partSize && uploadId.isEmpty()) {
                PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key);
                S3RequestUtil.configureEncryption(secretKey, putObjectRequestBuilder);

                try {
                    s3Client.putObject(putObjectRequestBuilder.build(),
                            // avoid extra memory copy
                            fromContentProvider(() -> new ByteArrayInputStream(slice.getBytes()), slice.length(), Mimetype.MIMETYPE_OCTET_STREAM));
                    return immediateVoidFuture();
                }
                catch (AwsServiceException e) {
                    throw new IOException(e);
                }
            }

            if (uploadId.isEmpty()) {
                uploadId = Optional.of(createMultipartUpload().uploadId());
            }

            CompletableFuture<CompletedPart> uploadFuture = uploadPart(uploadId.get(), slice);
            uploadFutures.add(uploadFuture);

            return asVoid(toListenableFuture(uploadFuture));
        }

        @Override
        public void close()
                throws IOException
        {
            if (uploadId.isEmpty()) {
                return;
            }

            try {
                List<CompletedPart> completedParts = uploadFutures.stream()
                        .map(CompletableFuture::join)
                        .sorted(Comparator.comparing(CompletedPart::partNumber))
                        .collect(toImmutableList());
                completeMultiUpload(uploadId.get(), completedParts);
            }
            catch (RuntimeException e) {
                abortUploadSuppressed(uploadId.get(), e);
                throw new IOException(e);
            }
        }

        private CreateMultipartUploadResponse createMultipartUpload()
        {
            CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key);
            S3RequestUtil.configureEncryption(secretKey, createMultipartUploadRequestBuilder);
            return s3Client.createMultipartUpload(createMultipartUploadRequestBuilder.build());
        }

        private CompletableFuture<CompletedPart> uploadPart(String uploadId, Slice slice)
        {
            currentPartNumber++;
            UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(currentPartNumber);
            S3RequestUtil.configureEncryption(secretKey, uploadPartRequestBuilder);
            UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();
            return s3AsyncClient.uploadPart(uploadPartRequest, DirectByteArrayAsyncRequestBody.fromByteBuffer(slice.toByteBuffer()))
                    .thenApply(uploadPartResponse -> CompletedPart.builder().eTag(uploadPartResponse.eTag()).partNumber(uploadPartRequest.partNumber()).build());
        }

        private void completeMultiUpload(String uploadId, List<CompletedPart> completedParts)
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
            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        }

        private void abortUpload(String uploadId)
        {
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .build();
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
        }

        @SuppressWarnings("ObjectEquality")
        private void abortUploadSuppressed(String uploadId, Throwable throwable)
        {
            try {
                abortUpload(uploadId);
            }
            catch (Throwable t) {
                if (throwable != t) {
                    throwable.addSuppressed(t);
                }
            }
        }
    }
}
