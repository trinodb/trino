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

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.endpoint.DefaultServiceEndpointBuilder;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
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
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeFutures.translateFailures;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeManager.PATH_SEPARATOR;
import static io.trino.plugin.exchange.filesystem.s3.S3FileSystemExchangeStorage.CompatibilityMode.GCP;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromByteBufferUnsafe;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_SUFFIX;

public class S3FileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final Logger log = Logger.get(S3FileSystemExchangeStorage.class);

    public enum CompatibilityMode
    {
        AWS,
        GCP
    }

    private final S3FileSystemExchangeStorageStats stats;
    private final Optional<Region> region;
    private final Optional<String> endpoint;
    private final int multiUploadPartSize;
    private final S3AsyncClient s3AsyncClient;
    private final StorageClass storageClass;
    private final CompatibilityMode compatibilityMode;

    // GCS specific
    private final Optional<Storage> gcsClient;
    private final Optional<ListeningExecutorService> gcsDeleteExecutor;

    @Inject
    public S3FileSystemExchangeStorage(S3FileSystemExchangeStorageStats stats, ExchangeS3Config config, CompatibilityMode compatibilityMode)
            throws IOException
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.region = config.getS3Region();
        this.endpoint = config.getS3Endpoint();
        this.multiUploadPartSize = toIntExact(config.getS3UploadPartSize().toBytes());
        this.storageClass = config.getStorageClass();
        this.compatibilityMode = requireNonNull(compatibilityMode, "compatibilityMode is null");

        AwsCredentialsProvider credentialsProvider = createAwsCredentialsProvider(config);
        RetryPolicy retryPolicy = RetryPolicy.builder(config.getRetryMode())
                .numRetries(config.getS3MaxErrorRetries())
                .build();
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .putAdvancedOption(USER_AGENT_PREFIX, "")
                .putAdvancedOption(USER_AGENT_SUFFIX, "Trino-exchange")
                .build();
        S3AsyncClient client = createS3AsyncClient(
                credentialsProvider,
                overrideConfig,
                config.isS3PathStyleAccess(),
                config.getAsyncClientConcurrency(),
                config.getAsyncClientMaxPendingConnectionAcquires(),
                config.getConnectionAcquisitionTimeout());
        this.s3AsyncClient = new S3AsyncClientWrapper(client)
        {
            @Override
            protected void handle(RequestType requestType, CompletableFuture<?> responseFuture)
            {
                stats.requestStarted(requestType);
                responseFuture.whenComplete((result, failure) -> {
                    if (failure != null && failure.getMessage() != null && failure.getMessage().contains("Maximum pending connection acquisitions exceeded")) {
                        log.error(failure, "Encountered 'Maximum pending connection acquisitions exceeded' error. Active requests: %s", stats.getActiveRequestsSummary());
                    }
                    stats.requestCompleted(requestType);
                });
            }
        };

        if (compatibilityMode == GCP) {
            Optional<String> gcsJsonKeyFilePath = config.getGcsJsonKeyFilePath();
            Optional<String> gcsJsonKey = config.getGcsJsonKey();
            verify(!(gcsJsonKeyFilePath.isPresent() && gcsJsonKey.isPresent()),
                    "gcsJsonKeyFilePath and gcsJsonKey shouldn't be set at the same time");
            if (gcsJsonKeyFilePath.isPresent()) {
                Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(gcsJsonKeyFilePath.get()));
                this.gcsClient = Optional.of(StorageOptions.newBuilder().setCredentials(credentials).build().getService());
            }
            else if (gcsJsonKey.isPresent()) {
                Credentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(gcsJsonKey.get().getBytes(StandardCharsets.UTF_8)));
                this.gcsClient = Optional.of(StorageOptions.newBuilder().setCredentials(credentials).build().getService());
            }
            else {
                this.gcsClient = Optional.of(StorageOptions.getDefaultInstance().getService());
            }
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    100,
                    100,
                    60L,
                    SECONDS,
                    new LinkedBlockingQueue<>(),
                    threadsNamed("gcs-delete-%s"));
            executor.allowCoreThreadTimeOut(true);
            this.gcsDeleteExecutor = Optional.of(listeningDecorator(executor));
        }
        else {
            this.gcsClient = Optional.empty();
            this.gcsDeleteExecutor = Optional.empty();
        }
    }

    @Override
    public void createDirectories(URI dir)
    {
        // Nothing to do for S3
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize)
    {
        return new S3ExchangeStorageReader(stats, s3AsyncClient, multiUploadPartSize, sourceFiles, maxPageStorageSize);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file)
    {
        String bucketName = getBucketName(file);
        String key = keyFromUri(file);

        return new S3ExchangeStorageWriter(stats, s3AsyncClient, bucketName, key, multiUploadPartSize, storageClass);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(getBucketName(file))
                .key(keyFromUri(file))
                .build();

        return stats.getCreateEmptyFile().record(translateFailures(toListenableFuture(s3AsyncClient.putObject(request, AsyncRequestBody.empty()))));
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        if (compatibilityMode == GCP) {
            return deleteRecursivelyGcp(directories);
        }

        ImmutableMultimap.Builder<String, ListenableFuture<List<String>>> bucketToListObjectsFuturesBuilder = ImmutableMultimap.builder();
        for (URI dir : directories) {
            ImmutableList.Builder<String> keys = ImmutableList.builder();
            ListenableFuture<List<String>> listObjectsFuture = Futures.transform(
                    toListenableFuture((listObjectsRecursively(dir)
                            .subscribe(listObjectsV2Response -> listObjectsV2Response.contents().stream()
                                    .map(S3Object::key)
                                    .forEach(keys::add)))),
                    ignored -> keys.build(),
                    directExecutor());
            bucketToListObjectsFuturesBuilder.put(getBucketName(dir), listObjectsFuture);
        }
        Multimap<String, ListenableFuture<List<String>>> bucketToListObjectsFutures = bucketToListObjectsFuturesBuilder.build();

        ImmutableList.Builder<ListenableFuture<List<DeleteObjectsResponse>>> deleteObjectsFutures = ImmutableList.builder();
        for (String bucketName : bucketToListObjectsFutures.keySet()) {
            deleteObjectsFutures.add(Futures.transformAsync(
                    Futures.allAsList(bucketToListObjectsFutures.get(bucketName)),
                    keys -> deleteObjects(
                            bucketName,
                            keys.stream()
                                    .flatMap(Collection::stream)
                                    .collect(toImmutableList())),
                    directExecutor()));
        }
        return translateFailures(Futures.allAsList(deleteObjectsFutures.build()));
    }

    private ListenableFuture<Void> deleteRecursivelyGcp(List<URI> directories)
    {
        // GCS is not compatible with S3's multi-object delete API https://cloud.google.com/storage/docs/migrating#methods-comparison
        Storage storage = gcsClient.orElseThrow(() -> new IllegalStateException("gcsClient is expected to be initialized"));
        ListeningExecutorService deleteExecutor = gcsDeleteExecutor.orElseThrow(() -> new IllegalStateException("gcsDeleteExecutor is expected to be initialized"));
        return stats.getDeleteRecursively().record(translateFailures(deleteExecutor.submit(() -> {
            StorageBatch batch = storage.batch();
            for (URI dir : directories) {
                Page<Blob> blobs = storage.list(getBucketName(dir), Storage.BlobListOption.prefix(keyFromUri(dir)));
                for (Blob blob : blobs.iterateAll()) {
                    batch.delete(blob.getBlobId());
                }
            }
            batch.submit();
        })));
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        ImmutableList.Builder<FileStatus> fileStatuses = ImmutableList.builder();
        return stats.getListFilesRecursively().record(Futures.transform(
                toListenableFuture((listObjectsRecursively(dir)
                        .subscribe(listObjectsV2Response -> {
                            for (S3Object s3Object : listObjectsV2Response.contents()) {
                                URI uri;
                                try {
                                    uri = new URI(dir.getScheme(), dir.getHost(), PATH_SEPARATOR + s3Object.key(), dir.getFragment());
                                }
                                catch (URISyntaxException e) {
                                    throw new IllegalArgumentException(e);
                                }
                                fileStatuses.add(new FileStatus(uri.toString(), s3Object.size()));
                            }
                        }))),
                ignored -> fileStatuses.build(),
                directExecutor()));
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
            closer.register(s3AsyncClient::close);
            gcsDeleteExecutor.ifPresent(listeningExecutorService -> closer.register(listeningExecutorService::shutdown));
        }
    }

    private ListObjectsV2Publisher listObjectsRecursively(URI dir)
    {
        checkArgument(isDirectory(dir), "listObjectsRecursively called on file uri %s", dir);

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(getBucketName(dir))
                .prefix(keyFromUri(dir))
                .build();

        return s3AsyncClient.listObjectsV2Paginator(request);
    }

    private ListenableFuture<List<DeleteObjectsResponse>> deleteObjects(String bucketName, List<String> keys)
    {
        List<List<String>> subList = Lists.partition(keys, 1000); //  deleteObjects has a limit of 1000
        stats.getDeleteObjectsEntriesCount().add(keys.size());
        return stats.getDeleteObjects().record(Futures.allAsList(subList.stream().map(list -> {
            DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(list.stream().map(key -> ObjectIdentifier.builder().key(key).build()).collect(toImmutableList())).build())
                    .build();
            return toListenableFuture(s3AsyncClient.deleteObjects(request));
        }).collect(toImmutableList())));
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
        String accessKey = config.getS3AwsAccessKey();
        String secretKey = config.getS3AwsSecretKey();

        if (accessKey == null && secretKey != null) {
            throw new IllegalArgumentException("AWS access key set but secret is not set; make sure you set exchange.s3.aws-secret-key config property");
        }

        if (accessKey != null && secretKey == null) {
            throw new IllegalArgumentException("AWS secret key set but access is not set; make sure you set exchange.s3.aws-access-key config property");
        }

        if (accessKey != null) {
            checkArgument(
                    config.getS3IamRole().isEmpty(),
                    "IAM role is not compatible with access key based authentication; make sure you set only one of exchange.s3.aws-access-key, exchange.s3.iam-role config properties");
            checkArgument(
                    config.getS3ExternalId().isEmpty(),
                    "External ID is not compatible with access key based authentication; make sure you set only one of exchange.s3.aws-access-key, exchange.s3.external-id config properties");

            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }

        if (config.getS3ExternalId().isPresent() && config.getS3IamRole().isEmpty()) {
            throw new IllegalArgumentException("External ID can only be used with IAM role based authentication; make sure you set exchange.s3.iam-role config property");
        }

        if (config.getS3IamRole().isPresent()) {
            AssumeRoleRequest.Builder assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(config.getS3IamRole().get())
                    .roleSessionName("trino-exchange");
            config.getS3ExternalId().ifPresent(assumeRoleRequest::externalId);

            StsClientBuilder stsClientBuilder = StsClient.builder();
            config.getS3Region().ifPresent(stsClientBuilder::region);

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClientBuilder.build())
                    .refreshRequest(assumeRoleRequest.build())
                    .asyncCredentialUpdateEnabled(true)
                    .build();
        }

        return DefaultCredentialsProvider.create();
    }

    private S3AsyncClient createS3AsyncClient(
            AwsCredentialsProvider credentialsProvider,
            ClientOverrideConfiguration overrideConfig,
            boolean isS3PathStyleAccess,
            int maxConcurrency,
            int maxPendingConnectionAcquires,
            Duration connectionAcquisitionTimeout)
    {
        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfig)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(isS3PathStyleAccess)
                        .build())
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(maxConcurrency)
                        .maxPendingConnectionAcquires(maxPendingConnectionAcquires)
                        .connectionAcquisitionTimeout(java.time.Duration.ofMillis(connectionAcquisitionTimeout.toMillis())))
                .endpointOverride(endpoint.map(URI::create).orElseGet(() -> new DefaultServiceEndpointBuilder("s3", "http")
                        .withRegion(region.orElseThrow(() -> new IllegalArgumentException("region is expected to be set")))
                        .getServiceEndpoint()));

        region.ifPresent(clientBuilder::region);

        return clientBuilder.build();
    }

    @ThreadSafe
    private static class S3ExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = instanceSize(S3ExchangeStorageReader.class);

        private final S3FileSystemExchangeStorageStats stats;
        private final S3AsyncClient s3AsyncClient;
        private final int partSize;
        private final int bufferSize;

        @GuardedBy("this")
        private final Queue<ExchangeSourceFile> sourceFiles;
        @GuardedBy("this")
        private ExchangeSourceFile currentFile;
        @GuardedBy("this")
        private long fileOffset;
        @GuardedBy("this")
        private SliceInput sliceInput;
        @GuardedBy("this")
        private int sliceSize = -1;
        private volatile boolean closed;
        private volatile long bufferRetainedSize;
        private volatile ListenableFuture<Void> inProgressReadFuture = immediateVoidFuture();

        public S3ExchangeStorageReader(
                S3FileSystemExchangeStorageStats stats,
                S3AsyncClient s3AsyncClient,
                int partSize,
                List<ExchangeSourceFile> sourceFiles,
                int maxPageStorageSize)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.s3AsyncClient = requireNonNull(s3AsyncClient, "s3AsyncClient is null");
            this.partSize = partSize;
            this.sourceFiles = new ArrayDeque<>(requireNonNull(sourceFiles, "sourceFiles is null"));
            // Make sure buffer can accommodate at least one complete Slice, and keep reads aligned to part boundaries
            this.bufferSize = maxPageStorageSize + partSize;

            // Safe publication of S3ExchangeStorageReader is required as it's a mutable class
            fillBuffer();
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed || !inProgressReadFuture.isDone()) {
                return null;
            }

            try {
                getFutureValue(inProgressReadFuture);
            }
            catch (RuntimeException e) {
                throw new IOException(e);
            }

            if (sliceSize < 0) {
                sliceSize = sliceInput.readInt();
            }
            Slice data = sliceInput.readSlice(sliceSize);

            if (sliceInput.available() > Integer.BYTES) {
                sliceSize = sliceInput.readInt();
                if (sliceInput.available() < sliceSize) {
                    fillBuffer();
                }
            }
            else {
                sliceSize = -1;
                fillBuffer();
            }

            return data;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            // rely on FileSystemExchangeSource implementation to wrap with nonCancellationPropagating
            return inProgressReadFuture;
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE + bufferRetainedSize;
        }

        @Override
        public boolean isFinished()
        {
            return closed;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;

            currentFile = null;
            sliceInput = null;
            bufferRetainedSize = 0;
            inProgressReadFuture.cancel(true);
            inProgressReadFuture = immediateVoidFuture(); // such that we don't retain reference to the buffer
        }

        @GuardedBy("this")
        private void fillBuffer()
        {
            if (currentFile == null || fileOffset == currentFile.getFileSize()) {
                currentFile = sourceFiles.poll();
                if (currentFile == null) {
                    close();
                    return;
                }
                fileOffset = 0;
            }

            byte[] buffer = new byte[bufferSize];
            int bufferFill = 0;
            if (sliceInput != null) {
                int length = sliceInput.available();
                sliceInput.readBytes(buffer, 0, length);
                bufferFill += length;
            }

            ImmutableList.Builder<ListenableFuture<GetObjectResponse>> getObjectFutures = ImmutableList.builder();
            while (true) {
                long fileSize = currentFile.getFileSize();
                // Make sure S3 read request byte ranges align with part sizes for best performance
                int readableParts = (buffer.length - bufferFill) / partSize;
                if (readableParts == 0) {
                    if (buffer.length - bufferFill >= fileSize - fileOffset) {
                        readableParts = 1;
                    }
                    else {
                        break;
                    }
                }

                String key = keyFromUri(currentFile.getFileUri());
                String bucketName = getBucketName(currentFile.getFileUri());
                for (int i = 0; i < readableParts && fileOffset < fileSize; ++i) {
                    int length = (int) min(partSize, fileSize - fileOffset);

                    GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
                            .key(key)
                            .bucket(bucketName)
                            .range("bytes=" + fileOffset + "-" + (fileOffset + length - 1));

                    ListenableFuture<GetObjectResponse> getObjectFuture = toListenableFuture(s3AsyncClient.getObject(getObjectRequestBuilder.build(),
                            BufferWriteAsyncResponseTransformer.toBufferWrite(buffer, bufferFill)));
                    stats.getGetObject().record(getObjectFuture);
                    stats.getGetObjectDataSizeInBytes().add(length);
                    getObjectFutures.add(getObjectFuture);
                    bufferFill += length;
                    fileOffset += length;
                }

                if (fileOffset == fileSize) {
                    currentFile = sourceFiles.poll();
                    if (currentFile == null) {
                        break;
                    }
                    fileOffset = 0;
                }
            }

            inProgressReadFuture = asVoid(Futures.allAsList(getObjectFutures.build()));
            sliceInput = Slices.wrappedBuffer(buffer, 0, bufferFill).getInput();
            bufferRetainedSize = sliceInput.getRetainedSize();
        }
    }

    @NotThreadSafe
    private static class S3ExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(S3ExchangeStorageWriter.class);

        private final S3FileSystemExchangeStorageStats stats;
        private final S3AsyncClient s3AsyncClient;
        private final String bucketName;
        private final String key;
        private final int partSize;
        private final StorageClass storageClass;

        private int currentPartNumber;
        private ListenableFuture<Void> directUploadFuture;
        private ListenableFuture<String> multiPartUploadIdFuture;
        private final List<ListenableFuture<CompletedPart>> multiPartUploadFutures = new ArrayList<>();
        private volatile boolean closed;

        public S3ExchangeStorageWriter(
                S3FileSystemExchangeStorageStats stats,
                S3AsyncClient s3AsyncClient,
                String bucketName,
                String key,
                int partSize,
                StorageClass storageClass)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.s3AsyncClient = requireNonNull(s3AsyncClient, "s3AsyncClient is null");
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.partSize = partSize;
            this.storageClass = requireNonNull(storageClass, "storageClass is null");
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
                        .key(key)
                        .storageClass(storageClass);
                directUploadFuture = translateFailures(toListenableFuture(s3AsyncClient.putObject(putObjectRequestBuilder.build(),
                        fromByteBufferUnsafe(slice.toByteBuffer()))));
                stats.getPutObject().record(directUploadFuture);
                stats.getPutObjectDataSizeInBytes().add(slice.length());
                return directUploadFuture;
            }

            if (multiPartUploadIdFuture == null) {
                multiPartUploadIdFuture = Futures.transform(createMultipartUpload(), CreateMultipartUploadResponse::uploadId, directExecutor());
            }

            int partNum = ++currentPartNumber;
            ListenableFuture<CompletedPart> uploadFuture = Futures.transformAsync(multiPartUploadIdFuture, uploadId -> uploadPart(uploadId, slice, partNum), directExecutor());
            multiPartUploadFutures.add(uploadFuture);

            return translateFailures(uploadFuture);
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

            ListenableFuture<Void> finishFuture = translateFailures(Futures.transformAsync(
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
            return translateFailures(Futures.transformAsync(multiPartUploadIdFuture, this::abortMultipartUpload, directExecutor()));
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
                    .key(key)
                    .storageClass(storageClass);
            return stats.getCreateMultipartUpload().record(toListenableFuture(s3AsyncClient.createMultipartUpload(createMultipartUploadRequestBuilder.build())));
        }

        private ListenableFuture<CompletedPart> uploadPart(String uploadId, Slice slice, int partNumber)
        {
            UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber);
            UploadPartRequest uploadPartRequest = uploadPartRequestBuilder.build();
            stats.getUploadPartDataSizeInBytes().add(slice.length());
            return stats.getUploadPart().record(Futures.transform(toListenableFuture(s3AsyncClient.uploadPart(uploadPartRequest, fromByteBufferUnsafe(slice.toByteBuffer()))),
                    uploadPartResponse -> CompletedPart.builder().eTag(uploadPartResponse.eTag()).partNumber(partNumber).build(), directExecutor()));
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
            stats.getCompleteMultipartUploadPartsCount().add(completedParts.size());
            return stats.getCompleteMultipartUpload().record(toListenableFuture(s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest)));
        }

        private ListenableFuture<AbortMultipartUploadResponse> abortMultipartUpload(String uploadId)
        {
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .build();
            return stats.getAbortMultipartUpload().record(toListenableFuture(s3AsyncClient.abortMultipartUpload(abortMultipartUploadRequest)));
        }
    }
}
