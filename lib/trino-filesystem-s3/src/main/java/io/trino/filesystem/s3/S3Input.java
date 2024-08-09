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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class S3Input
        implements TrinoInput
{
    private final Location location;
    private final S3Client client;
    private final GetObjectRequest request;
    private final S3Context context;
    private boolean closed;

    public S3Input(Location location, S3Client client, GetObjectRequest request, S3Context context)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return;
        }

        String range = "bytes=%s-%s".formatted(position, (position + length) - 1);
        GetObjectRequest.Builder builder = request.toBuilder().range(range);
        addEncryptionSettings(builder);
        GetObjectRequest rangeRequest = builder.build();

        int n = read(buffer, offset, length, rangeRequest);
        if (n < length) {
            throw new EOFException("Read %s of %s requested bytes: %s".formatted(n, length, location));
        }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return 0;
        }

        String range = "bytes=-%s".formatted(length);
        GetObjectRequest.Builder builder = request.toBuilder().range(range);
        addEncryptionSettings(builder);
        GetObjectRequest rangeRequest = builder.build();

        return read(buffer, offset, length, rangeRequest);
    }

    private void addEncryptionSettings(GetObjectRequest.Builder builder)
    {
        if (context.sseType() == S3FileSystemConfig.S3SseType.CUSTOMER) {
            builder.sseCustomerAlgorithm(context.sseCustomerKey().algorithm());
            builder.sseCustomerKey(context.sseCustomerKey().key());
            builder.sseCustomerKeyMD5(context.sseCustomerKey().md5());
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + location);
        }
    }

    private int read(byte[] buffer, int offset, int length, GetObjectRequest rangeRequest)
            throws IOException
    {
        try {
            return client.getObject(rangeRequest, (_, inputStream) -> {
                try {
                    return inputStream.readNBytes(buffer, offset, length);
                }
                catch (AbortedException _) {
                    throw new InterruptedIOException();
                }
                catch (IOException e) {
                    throw RetryableException.create("Error reading getObject response", e);
                }
            });
        }
        catch (NoSuchKeyException _) {
            throw new FileNotFoundException(location.toString());
        }
        catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + location, e);
        }
    }
}
