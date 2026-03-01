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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
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

final class S3OutputStream
        extends OutputStream
{
    private static final int BUFFER_SIZE = toIntExact(DataSize.of(128, KILOBYTE).toBytes());

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
    private final LinkedBuffer buffer;

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
        this.buffer = new LinkedBuffer(BUFFER_SIZE);
        verify(key.isEmpty() || context.s3SseContext().sseType() == NONE, "Encryption key cannot be used with SSE configuration");
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
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
        // skip multipart upload if there would only be one part
        if (finished && !multipartUploadStarted) {
            try {
                putObject(
                        client,
                        context,
                        location,
                        key,
                        false,
                        buffer.takeInputStream(),
                        buffer.size());
                buffer.reset();
                return;
            }
            catch (Throwable e) {
                failed = true;
                throw e;
            }
        }

        // the multipart upload API only allows the last part to be smaller than 5MB
        if ((buffer.size() >= partSize) || (finished && (buffer.size() > 0))) {
            int dataLength = buffer.size();
            @SuppressWarnings("resource")
            InputStream dataInputStream = buffer.takeInputStream();
            if (finished) {
                buffer.close();
            }
            else {
                buffer.reset();
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
            inProgressUploadFuture = supplyAsync(() -> uploadPage(dataInputStream, dataLength), uploadExecutor);
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

    private CompletedPart uploadPage(InputStream inputStream, int length)
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
                .contentLength((long) length)
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

        UploadPartResponse response = client.uploadPart(request, RequestBody.fromInputStream(inputStream, length));

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
            InputStream inputStream,
            int dataLength)
            throws IOException
    {
        PutObjectRequest request = PutObjectRequest.builder()
                .overrideConfiguration(context::applyCredentialProviderOverride)
                .acl(getCannedAcl(context.cannedAcl()))
                .requestPayer(context.requestPayer())
                .bucket(location.bucket())
                .key(location.key())
                .storageClass(toStorageClass(context.storageClass()))
                .contentLength((long) dataLength)
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
            client.putObject(request, RequestBody.fromInputStream(inputStream, dataLength));
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

    @VisibleForTesting
    static class LinkedBuffer
            implements Closeable
    {
        private final int bufferSize;
        private final List<InputStream> parts;
        private byte[] currentBuffer;
        private int currentOffset;

        public LinkedBuffer(int bufferSize)
        {
            this.bufferSize = bufferSize;
            this.parts = new ArrayList<>();
            this.currentBuffer = new byte[bufferSize];
            this.currentOffset = 0;
        }

        public void write(int b)
        {
            checkState(currentBuffer != null, "LinkedBuffer is closed");
            if (remainingCapacity() == 0) {
                parts.add(new ByteArrayInputStream(currentBuffer));
                resetBuffer();
            }
            currentBuffer[currentOffset++] = (byte) b;
        }

        public void write(byte[] bytes, int offset, int length)
        {
            checkState(currentBuffer != null, "LinkedBuffer is closed");
            while (length > 0) {
                int bytesToWrite = min(length, remainingCapacity());

                arraycopy(bytes, offset, currentBuffer, currentOffset, bytesToWrite);

                currentOffset += bytesToWrite;
                offset += bytesToWrite;
                length -= bytesToWrite;

                if (remainingCapacity() == 0) {
                    parts.add(new ByteArrayInputStream(currentBuffer));
                    resetBuffer();
                }
            }
        }

        public int size()
        {
            return parts.size() * bufferSize + currentOffset;
        }

        public int chunks()
        {
            return parts.size();
        }

        public InputStream takeInputStream()
        {
            if (currentOffset == 0) {
                return new SequenceInputStream(enumeration(ImmutableList.copyOf(parts)));
            }
            return new SequenceInputStream(enumeration(ImmutableList.<InputStream>builder()
                    .addAll(ImmutableList.copyOf(parts))
                    .add(new ByteArrayInputStream(Arrays.copyOf(currentBuffer, currentOffset)))
                    .build()));
        }

        public void reset()
        {
            currentBuffer = new byte[bufferSize];
            currentOffset = 0;
            parts.clear();
        }

        private int remainingCapacity()
        {
            return bufferSize - currentOffset;
        }

        private void resetBuffer()
        {
            currentBuffer = new byte[bufferSize];
            currentOffset = 0;
        }

        @Override
        public void close()
        {
            currentBuffer = null;
            currentOffset = 0;
            parts.clear();
        }
    }
}
