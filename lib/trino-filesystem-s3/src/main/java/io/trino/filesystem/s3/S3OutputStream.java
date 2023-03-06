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

import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.RequestPayer;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.primitives.Ints.constrainToRange;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AWS_KMS;

final class S3OutputStream
        extends OutputStream
{
    private final List<CompletedPart> parts = new ArrayList<>();
    private final LocalMemoryContext memoryContext;
    private final S3Client client;
    private final S3Location location;
    private final int partSize;
    private final RequestPayer requestPayer;
    private final S3SseType sseType;
    private final String sseKmsKeyId;

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

    public S3OutputStream(AggregatedMemoryContext memoryContext, S3Client client, S3Context context, S3Location location)
    {
        this.memoryContext = memoryContext.newLocalMemoryContext(S3OutputStream.class.getSimpleName());
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.partSize = context.partSize();
        this.requestPayer = context.requestPayer();
        this.sseType = context.sseType();
        this.sseKmsKeyId = context.sseKmsKeyId();
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
                target = constrainToRange(target, capacity, partSize);
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
                    .requestPayer(requestPayer)
                    .bucket(location.bucket())
                    .key(location.key())
                    .contentLength((long) bufferSize)
                    .applyMutation(builder -> {
                        switch (sseType) {
                            case NONE -> { /* ignored */ }
                            case S3 -> builder.serverSideEncryption(AES256);
                            case KMS -> builder.serverSideEncryption(AWS_KMS).ssekmsKeyId(sseKmsKeyId);
                        }
                    })
                    .build();

            ByteBuffer bytes = ByteBuffer.wrap(buffer, 0, bufferSize);

            try {
                client.putObject(request, RequestBody.fromByteBuffer(bytes));
                return;
            }
            catch (SdkException e) {
                failed = true;
                throw new IOException(e);
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
            inProgressUploadFuture = supplyAsync(() -> uploadPage(data, length));
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
                    .requestPayer(requestPayer)
                    .bucket(location.bucket())
                    .key(location.key())
                    .applyMutation(builder -> {
                        switch (sseType) {
                            case NONE -> { /* ignored */ }
                            case S3 -> builder.serverSideEncryption(AES256);
                            case KMS -> builder.serverSideEncryption(AWS_KMS).ssekmsKeyId(sseKmsKeyId);
                        }
                    })
                    .build();

            uploadId = Optional.of(client.createMultipartUpload(request).uploadId());
        }

        currentPartNumber++;
        UploadPartRequest request = UploadPartRequest.builder()
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .contentLength((long) length)
                .uploadId(uploadId.get())
                .partNumber(currentPartNumber)
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
                .requestPayer(requestPayer)
                .bucket(location.bucket())
                .key(location.key())
                .uploadId(uploadId)
                .multipartUpload(x -> x.parts(parts))
                .build();

        client.completeMultipartUpload(request);
    }

    private void abortUpload()
    {
        uploadId.map(id -> AbortMultipartUploadRequest.builder()
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
