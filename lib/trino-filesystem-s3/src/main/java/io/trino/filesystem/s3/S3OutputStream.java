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
package io.trino.filesystem.s3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileMayHaveAlreadyExistedException;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl.getCannedAcl;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.NONE;
import static io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType.toStorageClass;
import static io.trino.filesystem.s3.S3SseCUtils.encoded;
import static io.trino.filesystem.s3.S3SseCUtils.md5Checksum;
import static io.trino.filesystem.s3.S3SseRequestConfigurator.setEncryptionSettings;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static java.util.Collections.enumeration;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static software.amazon.awssdk.core.internal.util.Mimetype.MIMETYPE_OCTET_STREAM;

final class S3OutputStream
        extends OutputStream
{
    private static final int INITIAL_BUFFER_SIZE = toIntExact(DataSize.of(8, KILOBYTE).toBytes());
    private static final int MAXIMUM_BUFFER_SIZE = toIntExact(DataSize.of(512, KILOBYTE).toBytes());

    private final List<CompletedPart> parts = new ArrayList<>();
    private final LocalMemoryContext memoryContext;
    private final Executor uploadExecutor;
    private final S3Client client;
    private final S3Location location;
    private final S3Context context;
    private final int partSize;
    private final RequestPayer requestPayer;
    private final StorageClass storageClass;
    private final ObjectCannedACL cannedAcl;
    private final Optional<EncryptionKey> key;
    private LinkedBuffer buffer;

    private int currentPartNumber;

    private boolean closed;
    private boolean failed;
    private boolean multipartUploadStarted;
    private Future<CompletedPart> inProgressUploadFuture;

    // Mutated by background thread which does the multipart upload.
    // Read by both main thread and background thread.
    // Visibility is ensured by calling get() on inProgressUploadFuture.
    private Optional<String> uploadId = Optional.empty();

    public S3OutputStream(AggregatedMemoryContext memoryContext, Executor uploadExecutor, S3Client client, S3Context context, S3Location location, Optional<EncryptionKey> key)
    {
        this.memoryContext = memoryContext.newLocalMemoryContext(S3OutputStream.class.getSimpleName());
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.context = requireNonNull(context, "context is null");
        this.partSize = context.partSize();
        this.requestPayer = context.requestPayer();
        this.storageClass = toStorageClass(context.storageClass());
        this.cannedAcl = getCannedAcl(context.cannedAcl());
        this.key = requireNonNull(key, "key is null");
        this.buffer = new LinkedBuffer(INITIAL_BUFFER_SIZE, MAXIMUM_BUFFER_SIZE);
        verify(key.isEmpty() || context.s3SseContext().sseType() == NONE, "Encryption key cannot be used with SSE configuration");
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        buffer.write(b);
        memoryContext.setBytes(buffer.size());
        flushBuffer(false);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();
        // make sure we don't exceed the part size
        while (length > 0) {
            int capacity = partSize - buffer.size();
            if (capacity >= length) {
                buffer.write(bytes, offset, length);
                memoryContext.setBytes(buffer.size());
                break;
            }
            buffer.write(bytes, offset, capacity);
            memoryContext.setBytes(buffer.size());
            flushBuffer(false);
            offset += capacity;
            length -= capacity;
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        flushBuffer(false);
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        if (failed) {
            try {
                abortUpload();
                return;
            }
            catch (SdkException e) {
                throw new IOException(e);
            }
        }

        try {
            flushBuffer(true);
            memoryContext.close();
            waitForPreviousUploadFinish();
        }
        catch (IOException | RuntimeException e) {
            abortUploadSuppressed(e);
            throw e;
        }

        try {
            uploadId.ifPresent(this::finishUpload);
        }
        catch (SdkException e) {
            abortUploadSuppressed(e);
            // when `location` already exists, the operation will fail with `412 Precondition Failed`
            if (e instanceof S3Exception s3Exception && s3Exception.statusCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(location.toString());
            }
            throw new IOException(e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    private void flushBuffer(boolean finished)
            throws IOException
    {
        DataStreamProvider dataStreamProvider = buffer;
        // skip multipart upload if there would only be one part
        if (finished && !multipartUploadStarted) {
            try {
                putObject(
                        client,
                        context,
                        location,
                        key,
                        false,
                        dataStreamProvider);
                buffer = new LinkedBuffer(INITIAL_BUFFER_SIZE, MAXIMUM_BUFFER_SIZE);
                return;
            }
            catch (Throwable e) {
                failed = true;
                throw e;
            }
        }

        // the multipart upload API only allows the last part to be smaller than 5MB
        if ((buffer.size() >= partSize) || (finished && (buffer.size() > 0))) {
            if (finished) {
                buffer = null;
            }
            else {
                buffer = new LinkedBuffer(INITIAL_BUFFER_SIZE, MAXIMUM_BUFFER_SIZE);
            }
            memoryContext.setBytes(0);

            try {
                waitForPreviousUploadFinish();
            }
            catch (IOException e) {
                failed = true;
                abortUploadSuppressed(e);
                throw e;
            }
            multipartUploadStarted = true;
            inProgressUploadFuture = supplyAsync(() -> uploadPage(dataStreamProvider), uploadExecutor);
        }
    }

    @VisibleForTesting
    void waitForPreviousUploadFinish()
            throws IOException
    {
        if (inProgressUploadFuture == null) {
            return;
        }

        try {
            inProgressUploadFuture.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        catch (ExecutionException e) {
            throw new IOException("Streaming upload failed", e);
        }
    }

    private CompletedPart uploadPage(DataStreamProvider dataStreamProvider)
    {
        if (uploadId.isEmpty()) {
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                    .overrideConfiguration(context::applyCredentialProviderOverride)
                    .acl(cannedAcl)
                    .requestPayer(requestPayer)
                    .bucket(location.bucket())
                    .key(location.key())
                    .storageClass(storageClass)
                    .applyMutation(builder ->
                            key.ifPresentOrElse(
                                    encryption ->
                                            builder.sseCustomerKey(encoded(encryption))
                                                    .sseCustomerAlgorithm(encryption.algorithm())
                                                    .sseCustomerKeyMD5(md5Checksum(encryption)),
                                    () -> setEncryptionSettings(builder, context.s3SseContext())))
                    .build();

            uploadId = Optional.of(client.createMultipartUpload(request).uploadId());
        }

        currentPartNumber++;
        UploadPartRequest request = UploadPartRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .contentLength((long) dataStreamProvider.size())
                .uploadId(uploadId.get())
                .partNumber(currentPartNumber)
                .applyMutation(builder ->
                        key.ifPresentOrElse(
                                encryption ->
                                        builder.sseCustomerKey(encoded(encryption))
                                                .sseCustomerAlgorithm(encryption.algorithm())
                                                .sseCustomerKeyMD5(md5Checksum(encryption)),
                                () -> setEncryptionSettings(builder, context.s3SseContext())))
                .build();

        UploadPartResponse response = client.uploadPart(request, RequestBody.fromContentProvider(dataStreamProvider::takeInputStream, dataStreamProvider.size(), MIMETYPE_OCTET_STREAM));

        CompletedPart part = CompletedPart.builder()
                .partNumber(currentPartNumber)
                .eTag(response.eTag())
                .build();

        parts.add(part);
        return part;
    }

    private void finishUpload(String uploadId)
    {
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .uploadId(uploadId)
                .multipartUpload(x -> x.parts(parts))
                .applyMutation(builder -> key.ifPresentOrElse(
                        encryption ->
                                builder.sseCustomerKey(encoded(encryption))
                                        .sseCustomerAlgorithm(encryption.algorithm())
                                        .sseCustomerKeyMD5(md5Checksum(encryption)),
                        () -> setEncryptionSettings(builder, context.s3SseContext())))
                .build();

        client.completeMultipartUpload(request);
    }

    private void abortUpload()
    {
        uploadId.map(id -> AbortMultipartUploadRequest.builder()
                        .overrideConfiguration(context::applyCredentialProviderOverride)
                        .requestPayer(requestPayer)
                        .bucket(location.bucket())
                        .key(location.key())
                        .uploadId(id)
                        .build())
                .ifPresent(client::abortMultipartUpload);
    }

    @SuppressWarnings("ObjectEquality")
    private void abortUploadSuppressed(Throwable throwable)
    {
        try {
            abortUpload();
        }
        catch (Throwable t) {
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }

    static void putObject(
            S3Client client,
            S3Context context,
            S3Location location,
            Optional<EncryptionKey> key,
            boolean exclusiveCreate,
            DataStreamProvider dataStreamProvider)
            throws IOException
    {
        PutObjectRequest request = PutObjectRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .acl(getCannedAcl(context.cannedAcl()))
                .requestPayer(context.requestPayer())
                .bucket(location.bucket())
                .key(location.key())
                .storageClass(toStorageClass(context.storageClass()))
                .contentLength((long) dataStreamProvider.size())
                .applyMutation(builder -> {
                    if (exclusiveCreate) {
                        builder.ifNoneMatch("*");
                    }
                    key.ifPresent(encryption -> {
                        builder.sseCustomerKey(encoded(encryption));
                        builder.sseCustomerAlgorithm(encryption.algorithm());
                        builder.sseCustomerKeyMD5(md5Checksum(encryption));
                    });
                    setEncryptionSettings(builder, context.s3SseContext());
                })
                .build();

        try {
            client.putObject(request, RequestBody.fromContentProvider(dataStreamProvider::takeInputStream, dataStreamProvider.size(), MIMETYPE_OCTET_STREAM));
        }
        catch (SdkException putObjectException) {
            // When `location` already exists, the operation will fail with `412 Precondition Failed`
            // This is possible when
            // - object already existed
            // - object was created by us in a request, but then client side retry logic retried the request
            boolean objectAlreadyExists = putObjectException instanceof S3Exception s3Exception &&
                    s3Exception.statusCode() == HTTP_PRECON_FAILED;
            if (objectAlreadyExists) {
                if (exclusiveCreate && requireNonNullElse(putObjectException.numAttempts(), 0) > 1) {
                    // The object might have been created by a previous attempt of AWS SDK's implicit retries
                    // Signal the uncertainty to the caller.
                    throw new FileMayHaveAlreadyExistedException("Put failed for bucket [%s] key [%s] but provenance could not be verified".formatted(location.bucket(), location.key()), putObjectException);
                }
                throw new FileAlreadyExistsException(location.toString());
            }
            throw new IOException("Put failed for bucket [%s] key [%s]: %s".formatted(location.bucket(), location.key(), putObjectException), putObjectException);
        }
    }

    interface DataStreamProvider
    {
        InputStream takeInputStream();

        int size();
    }

    record ByteArrayStreamProvider(byte[] data)
            implements DataStreamProvider
    {
        @Override
        public InputStream takeInputStream()
        {
            return new ByteArrayInputStream(data);
        }

        @Override
        public int size()
        {
            return data.length;
        }
    }

    @VisibleForTesting
    static class LinkedBuffer
            implements DataStreamProvider
    {
        final int initialBufferSize;
        final int maxBufferSize;
        List<InputStream> parts;
        int currentBufferSize;
        byte[] currentBuffer;
        int currentOffset;
        int totalSize;

        public LinkedBuffer(int initialBufferSize, int maxBufferSize)
        {
            checkArgument(initialBufferSize <= maxBufferSize, "initialBufferSize must be less than or equal to maxBufferSize");
            this.initialBufferSize = initialBufferSize;
            this.maxBufferSize = maxBufferSize;
            this.parts = new ArrayList<>();
            this.currentBufferSize = initialBufferSize;
            this.currentBuffer = new byte[initialBufferSize];
            this.currentOffset = 0;
            this.totalSize = 0;
        }

        @SuppressWarnings("NumericCastThatLosesPrecision")
        public void write(int b)
        {
            if (remainingCapacity() == 0) {
                parts.add(new ByteArrayInputStream(currentBuffer));
                resetBuffer();
            }
            currentBuffer[currentOffset] = (byte) b;
            currentOffset++;
            totalSize++;
        }

        public void write(byte[] bytes, int offset, int length)
        {
            writeInternal(bytes, offset, length);
        }

        @Override
        public int size()
        {
            return totalSize;
        }

        @Override
        public InputStream takeInputStream()
        {
            if (currentOffset == 0) {
                return new SequenceInputStream(enumeration(parts));
            }
            return new SequenceInputStream(enumeration(ImmutableList.<InputStream>builder()
                    .addAll(parts)
                    .add(new ByteArrayInputStream(currentBuffer, 0, currentOffset))
                    .build()));
        }

        public void reset()
        {
            currentBufferSize = initialBufferSize;
            currentBuffer = new byte[currentBufferSize];
            currentOffset = 0;
            totalSize = 0;
            parts = new ArrayList<>();
        }

        private int remainingCapacity()
        {
            return currentBufferSize - currentOffset;
        }

        private void resetBuffer()
        {
            // scale buffer capacity by 2 or up to the maximum allowed
            currentBufferSize = min(currentBufferSize * 2, maxBufferSize);
            currentBuffer = new byte[currentBufferSize];
            currentOffset = 0;
        }

        private void writeInternal(byte[] srcBytes, int srcOffset, int srcLength)
        {
            while (srcLength > 0) {
                int bytesToWrite = min(srcLength, remainingCapacity());

                arraycopy(srcBytes, srcOffset, currentBuffer, currentOffset, bytesToWrite);

                currentOffset += bytesToWrite;
                totalSize += bytesToWrite;
                srcOffset += bytesToWrite;
                srcLength -= bytesToWrite;

                if (remainingCapacity() == 0) {
                    parts.add(new ByteArrayInputStream(currentBuffer));
                    resetBuffer();
                }
            }
        }
    }
}
