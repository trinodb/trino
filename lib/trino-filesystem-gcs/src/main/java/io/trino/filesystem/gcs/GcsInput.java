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
package io.trino.filesystem.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.StorageException;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.filesystem.gcs.GcsUtils.encodedKey;
import static io.trino.filesystem.gcs.GcsUtils.getBlobOrThrow;
import static io.trino.filesystem.gcs.GcsUtils.getReadChannel;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class GcsInput
        implements TrinoInput
{
    private final GcsLocation location;
    private final Storage storage;
    private final OptionalLong length;
    private final Optional<EncryptionKey> key;
    private boolean closed;

    public GcsInput(GcsLocation location, Storage storage, OptionalLong length, Optional<EncryptionKey> key)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.length = requireNonNull(length, "length is null");
        this.key = requireNonNull(key, "key is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return;
        }

        try (ReadChannel readChannel = openReadChannel(position, bufferLength)) {
            int readSize = readNBytes(readChannel, buffer, bufferOffset, bufferLength);
            if (readSize != bufferLength) {
                throw new EOFException("End of file reached before reading fully: " + location);
            }
        }
        catch (IOException | RuntimeException e) {
            throw handleReadException(e);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return 0;
        }

        try (ReadChannel readChannel = openTailReadChannel(bufferLength)) {
            return readNBytes(readChannel, buffer, bufferOffset, bufferLength);
        }
        catch (IOException | RuntimeException e) {
            throw handleReadException(e);
        }
    }

    private ReadChannel openReadChannel(long position, int bufferLength)
            throws IOException
    {
        OptionalLong limit = readLimit(position, bufferLength, length);
        if (length.isPresent() && position < length.orElseThrow()) {
            return getReadChannel(storage, BlobId.of(location.bucket(), location.path()), location, length.orElseThrow(), position, bufferLength, limit, key);
        }
        Blob blob = getBlobOrThrow(storage, location, blobGetOptions());
        return getReadChannel(blob, location, position, bufferLength, limit, key);
    }

    private ReadChannel openTailReadChannel(int bufferLength)
            throws IOException
    {
        if (length.isPresent() && length.orElseThrow() > 0) {
            return openReadChannel(max(0, length.orElseThrow() - bufferLength), bufferLength);
        }
        Blob blob = getBlobOrThrow(storage, location, blobGetOptions());
        long offset = max(0, length.orElse(blob.getSize()) - bufferLength);
        return getReadChannel(blob, location, offset, bufferLength, readLimit(offset, bufferLength, OptionalLong.of(blob.getSize())), key);
    }

    private IOException handleReadException(Exception exception)
            throws IOException
    {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof StorageException storageException) {
                if (storageException.getCode() == 404) {
                    FileNotFoundException fileNotFoundException = new FileNotFoundException("File %s not found".formatted(location));
                    fileNotFoundException.initCause(exception);
                    return fileNotFoundException;
                }
                return handleGcsException(storageException, "reading file", location);
            }
        }
        if (exception instanceof IOException ioException) {
            return ioException;
        }
        return handleGcsException((RuntimeException) exception, "reading file", location);
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    private int readNBytes(ReadChannel readChannel, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ByteBuffer wrappedBuffer = ByteBuffer.wrap(buffer, bufferOffset, bufferLength);
        int readSize = 0;
        while (readSize < bufferLength) {
            int bytesRead = readChannel.read(wrappedBuffer);
            if (bytesRead == -1) {
                break;
            }
            readSize += bytesRead;
        }
        return readSize;
    }

    private BlobGetOption[] blobGetOptions()
    {
        return key
                .map(encryption -> new BlobGetOption[] {BlobGetOption.decryptionKey(encodedKey(encryption))})
                .orElseGet(() -> new BlobGetOption[0]);
    }

    private static OptionalLong readLimit(long position, int length, OptionalLong fileSize)
    {
        long limit = addExact(position, length);
        if (fileSize.isPresent()) {
            limit = min(limit, fileSize.orElseThrow());
        }
        return OptionalLong.of(limit);
    }
}
