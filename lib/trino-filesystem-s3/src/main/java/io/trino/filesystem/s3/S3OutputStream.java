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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.google.common.base.Verify.verify;
import static io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl.getCannedAcl;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.NONE;
import static io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType.toStorageClass;
import static io.trino.filesystem.s3.S3SseCUtils.encoded;
import static io.trino.filesystem.s3.S3SseCUtils.md5Checksum;
import static io.trino.filesystem.s3.S3SseRequestConfigurator.setEncryptionSettings;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;

final class S3OutputStream
        extends OutputStream
{
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
    private final boolean exclusiveCreate;
    private final Optional<EncryptionKey> key;

    private int currentPartNumber;
    private byte[] buffer = new byte[0];
    private int bufferSize;
    private int initialBufferSize = 64;

    private boolean closed;
    private boolean failed;
    private boolean multipartUploadStarted;
    private Future<CompletedPart> inProgressUploadFuture;

    // Mutated by background thread which does the multipart upload.
    // Read by both main thread and background thread.
    // Visibility is ensured by calling get() on inProgressUploadFuture.
    private Optional<String> uploadId = Optional.empty();

    public S3OutputStream(AggregatedMemoryContext memoryContext, Executor uploadExecutor, S3Client client, S3Context context, S3Location location, boolean exclusiveCreate, Optional<EncryptionKey> key)
    {
        this.memoryContext = memoryContext.newLocalMemoryContext(S3OutputStream.class.getSimpleName());
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.exclusiveCreate = exclusiveCreate;
        this.context = requireNonNull(context, "context is null");
        this.partSize = context.partSize();
        this.requestPayer = context.requestPayer();
        this.storageClass = toStorageClass(context.storageClass());
        this.cannedAcl = getCannedAcl(context.cannedAcl());
        this.key = requireNonNull(key, "key is null");

        verify(key.isEmpty() || context.s3SseContext().sseType() == NONE, "Encryption key cannot be used with SSE configuration");
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        ensureCapacity(1);
        buffer[bufferSize] = (byte) b;
        bufferSize++;
        flushBuffer(false);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();

        while (length > 0) {
            ensureCapacity(length);

            int copied = min(buffer.length - bufferSize, length);
            arraycopy(bytes, offset, buffer, bufferSize, copied);
            bufferSize += copied;

            flushBuffer(false);

            offset += copied;
            length -= copied;
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

    private void ensureCapacity(int extra)
    {
        int capacity = min(partSize, bufferSize + extra);
        if (buffer.length < capacity) {
            int target = max(buffer.length, initialBufferSize);
            if (target < capacity) {
                target += target / 2; // increase 50%
                target = clamp(target, capacity, partSize);
            }
            buffer = Arrays.copyOf(buffer, target);
            memoryContext.setBytes(buffer.length);
        }
    }

    private void flushBuffer(boolean finished)
            throws IOException
    {
        // skip multipart upload if there would only be one part
        if (finished && !multipartUploadStarted) {
            PutObjectRequest request = PutObjectRequest.builder()
                    .overrideConfiguration(context::applyCredentialProviderOverride)
                    .acl(cannedAcl)
                    .requestPayer(requestPayer)
                    .bucket(location.bucket())
                    .key(location.key())
                    .storageClass(storageClass)
                    .contentLength((long) bufferSize)
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

            ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, bufferSize);

            try {
                client.putObject(request, RequestBody.fromByteBuffer(bytes));
                return;
            }
            catch (S3Exception e) {
                failed = true;
                // when `location` already exists, the operation will fail with `412 Precondition Failed`
                if (e.statusCode() == HTTP_PRECON_FAILED) {
                    throw new FileAlreadyExistsException(location.toString());
                }
                throw new IOException("Put failed for bucket [%s] key [%s]: %s".formatted(location.bucket(), location.key(), e), e);
            }
            catch (SdkException e) {
                failed = true;
                throw new IOException("Put failed for bucket [%s] key [%s]: %s".formatted(location.bucket(), location.key(), e), e);
            }
        }

        // the multipart upload API only allows the last part to be smaller than 5MB
        if ((bufferSize == partSize) || (finished && (bufferSize > 0))) {
            byte[] data = buffer;
            int length = bufferSize;

            if (finished) {
                this.buffer = null;
            }
            else {
                this.buffer = new byte[0];
                this.initialBufferSize = partSize;
                bufferSize = 0;
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
            inProgressUploadFuture = supplyAsync(() -> uploadPage(data, length), uploadExecutor);
        }
    }

    private void waitForPreviousUploadFinish()
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

    private CompletedPart uploadPage(byte[] data, int length)
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

        ByteBuffer bytes = ByteBuffer.wrap(data, 0, length);

        UploadPartResponse response = client.uploadPart(request, RequestBody.fromByteBuffer(bytes));

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
                .applyMutation(builder -> {
                    key.ifPresentOrElse(
                            encryption ->
                                    builder.sseCustomerKey(encoded(encryption))
                                            .sseCustomerAlgorithm(encryption.algorithm())
                                            .sseCustomerKeyMD5(md5Checksum(encryption)),
                            () -> setEncryptionSettings(builder, context.s3SseContext()));
                    if (exclusiveCreate) {
                        builder.ifNoneMatch("*");
                    }
                })
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
}
