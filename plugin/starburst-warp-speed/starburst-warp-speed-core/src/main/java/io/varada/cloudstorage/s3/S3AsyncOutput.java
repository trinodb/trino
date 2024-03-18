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
package io.varada.cloudstorage.s3;

import io.trino.filesystem.Location;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.varada.cloudstorage.s3.S3Utils.handleAwsException;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.services.s3.model.ServerSideEncryption.AES256;

final class S3AsyncOutput
        implements Closeable
{
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // S3 requirement
    private static final long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024; // S3 requirement

    private final S3AsyncClient client;
    private final S3Location source;
    private final S3Location destination;

    private boolean closed;
    private boolean failed;
    private String uploadId;
    private int partNumber;
    private final List<CompletedPart> parts = new ArrayList<>();

    public S3AsyncOutput(S3AsyncClient client, Location source, Location destination)
    {
        this.client = requireNonNull(client, "client is null");
        source.verifyValidFileLocation();
        this.source = new S3Location(source);
        destination.verifyValidFileLocation();
        this.destination = new S3Location(destination);
    }

    public void writeTail(long position, byte[] buffer)
            throws IOException
    {
        writeTail(position, buffer, 0, buffer.length);
    }

    public void writeTail(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (position > 0 && position < MIN_PART_SIZE) {
            throw new IOException("S3 does not support partSize less than 5 GiB");
        }
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return;
        }

        if (uploadId == null) {
            getUploadId();
        }

        partNumber = 1;
        copyParts(position);

        CompletedPart part = uploadPart(buffer, offset, length);

        parts.add(part);
        partNumber++;

        finishUpload();
        uploadId = null;
    }

    private void getUploadId()
            throws IOException
    {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(destination.bucket())
                .key(destination.key())
                .serverSideEncryption(AES256)
                .build();

        try {
            CreateMultipartUploadResponse response = client.createMultipartUpload(request).join();
            uploadId = response.uploadId();
        }
        catch (RuntimeException e) {
            throw handleAwsException(e, "createMultipartUpload failed", destination);
        }
    }

    private void copyParts(long size)
            throws IOException
    {
        long offset = 0;
        while (size > 0) {
            long length = min(size, MAX_PART_SIZE);
            CompletedPart part = uploadPartCopy(offset, length);

            parts.add(part);
            partNumber++;

            size -= length;
            offset += length;
        }
    }

    private CompletedPart uploadPartCopy(long offset, long length)
            throws IOException
    {
        UploadPartCopyRequest request = UploadPartCopyRequest.builder()
                .sourceBucket(source.bucket())
                .sourceKey(source.key())
                .copySourceRange("bytes=%d-%d".formatted(offset, offset + length - 1))
                .destinationBucket(destination.bucket())
                .destinationKey(destination.key())
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        try {
            UploadPartCopyResponse response = client.uploadPartCopy(request).join();

            return CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(response.copyPartResult().eTag())
                    .build();
        }
        catch (RuntimeException e) {
            failed = true;
            throw handleAwsException(e, "uploadPartCopy failed", destination);
        }
    }

    private CompletedPart uploadPart(byte[] buffer, int offset, int length)
            throws IOException
    {
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(destination.bucket())
                .key(destination.key())
                .contentLength((long) length)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        ByteBuffer bytes = ByteBuffer.wrap(buffer, offset, length);
        AsyncRequestBody requestBody = AsyncRequestBody.fromByteBuffer(bytes);

        try {
            UploadPartResponse response = client.uploadPart(request, requestBody).join();

            return CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(response.eTag())
                    .build();
        }
        catch (RuntimeException e) {
            failed = true;
            throw handleAwsException(e, "uploadPart failed", destination);
        }
    }

    private void finishUpload()
            throws IOException
    {
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
                .bucket(destination.bucket())
                .key(destination.key())
                .uploadId(uploadId)
                .multipartUpload(x -> x.parts(parts))
                .build();

        try {
            client.completeMultipartUpload(request).join();
        }
        catch (RuntimeException e) {
            failed = true;
            throw handleAwsException(e, "completeMultipartUpload failed", destination);
        }
    }

    private void abortUpload()
            throws IOException
    {
        AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                .bucket(destination.bucket())
                .key(destination.key())
                .uploadId(uploadId)
                .build();

        try {
            client.abortMultipartUpload(request).join();
        }
        catch (RuntimeException e) {
            failed = true;
            throw handleAwsException(e, "abortMultipartUpload failed", destination);
        }
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
            abortUpload();
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output closed: " + destination);
        }
    }
}
