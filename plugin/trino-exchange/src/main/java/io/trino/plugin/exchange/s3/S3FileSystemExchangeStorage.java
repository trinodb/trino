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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.plugin.exchange.ExchangeSourceFile;
import io.trino.plugin.exchange.ExchangeStorageReader;
import io.trino.plugin.exchange.ExchangeStorageWriter;
import io.trino.plugin.exchange.FileStatus;
import io.trino.plugin.exchange.FileSystemExchangeStorage;
import org.openjdk.jol.info.ClassLayout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
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
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.SecretKey;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

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
import static java.lang.Math.min;
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
    private final StorageClass storageClass;

    @Inject
    public S3FileSystemExchangeStorage(ExchangeS3Config config)
    {
        requireNonNull(config, "config is null");
        this.region = config.getS3Region();
        this.endpoint = config.getS3Endpoint();
        this.multiUploadPartSize = toIntExact(config.getS3UploadPartSize().toBytes());
        this.storageClass = config.getStorageClass();

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
    public ExchangeStorageReader createExchangeStorageReader(Queue<ExchangeSourceFile> sourceFiles, int maxPageStorageSize)
    {
        return new S3ExchangeStorageReader(s3AsyncClient, sourceFiles, multiUploadPartSize, maxPageStorageSize);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file, Optional<SecretKey> secretKey)
    {
        String bucketName = getBucketName(file);
        String key = keyFromUri(file);

        return new S3ExchangeStorageWriter(s3AsyncClient, bucketName, key, multiUploadPartSize, secretKey, storageClass);
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

        if (config.isS3UseWebIdentityTokenCredentials()) {
            return WebIdentityTokenFileCredentialsProvider.create();
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

    @ThreadSafe
    private static class S3ExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(S3ExchangeStorageReader.class).instanceSize();

        private final S3AsyncClient s3AsyncClient;
        private final Queue<ExchangeSourceFile> sourceFiles;
        private final int partSize;
        private final int bufferSize;

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
                S3AsyncClient s3AsyncClient,
                Queue<ExchangeSourceFile> sourceFiles,
                int partSize,
                int maxPageStorageSize)
        {
            this.s3AsyncClient = requireNonNull(s3AsyncClient, "s3AsyncClient is null");
            this.sourceFiles = requireNonNull(sourceFiles, "sourceFiles is null");
            this.partSize = partSize;
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
                Optional<SecretKey> secretKey = currentFile.getSecretKey();
                for (int i = 0; i < readableParts && fileOffset < fileSize; ++i) {
                    int length = (int) min(partSize, fileSize - fileOffset);
                    int partNumber = (int) (fileOffset / partSize + 1);

                    GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
                            .key(key)
                            .bucket(bucketName)
                            .partNumber(partNumber);
                    configureEncryption(secretKey, getObjectRequestBuilder);

                    getObjectFutures.add(toListenableFuture(s3AsyncClient.getObject(getObjectRequestBuilder.build(),
                            BufferWriteAsyncResponseTransformer.toBufferWrite(buffer, bufferFill))));
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
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(S3ExchangeStorageWriter.class).instanceSize();

        private final S3AsyncClient s3AsyncClient;
        private final String bucketName;
        private final String key;
        private final int partSize;
        private final Optional<SecretKey> secretKey;
        private final StorageClass storageClass;

        private int currentPartNumber;
        private ListenableFuture<Void> directUploadFuture;
        private ListenableFuture<String> multiPartUploadIdFuture;
        private final List<ListenableFuture<CompletedPart>> multiPartUploadFutures = new ArrayList<>();
        private volatile boolean closed;

        public S3ExchangeStorageWriter(S3AsyncClient s3AsyncClient, String bucketName, String key, int partSize, Optional<SecretKey> secretKey, StorageClass storageClass)
        {
            this.s3AsyncClient = requireNonNull(s3AsyncClient, "s3AsyncClient is null");
            this.bucketName = requireNonNull(bucketName, "bucketName is null");
            this.key = requireNonNull(key, "key is null");
            this.partSize = partSize;
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
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
                    .key(key)
                    .storageClass(storageClass);
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
