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

import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.OptionalLong;

import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static io.trino.filesystem.gcs.GcsUtils.openGcsInputStream;
import static java.lang.Math.clamp;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class AnalyticsCoreGcsInputStream
        extends TrinoInputStream
{
    private final GcsLocation location;
    private final GcsFileInfo fileInfo;
    private final long fileSize;
    private final GcsFileSystem gcsFileSystem;
    private GoogleCloudStorageInputStream stream;
    private boolean closed;

    public AnalyticsCoreGcsInputStream(GcsLocation location, Storage storage, GcsFileInfo fileInfo, GcsFileSystem gcsFileSystem, OptionalLong predeclaredLength)
            throws IOException
    {
        this.location = requireNonNull(location, "location is null");
        requireNonNull(storage, "storage is null");
        this.fileInfo = requireNonNull(fileInfo, "fileInfo is null");
        this.fileSize = predeclaredLength.orElse(fileInfo.getItemInfo().getSize());
        this.gcsFileSystem = requireNonNull(gcsFileSystem, "gcsFileSystem is null");
        openStream();
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return stream.available();
    }

    @Override
    public long getPosition()
            throws IOException
    {
        ensureOpen();
        return stream.getPos();
    }

    @Override
    public void seek(long newPosition)
            throws IOException
    {
        ensureOpen();
        if (newPosition < 0) {
            throw new IOException("Negative seek offset");
        }
        if (newPosition > fileSize) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(newPosition, fileSize, location));
        }
        stream.seek(newPosition);
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        try {
            return stream.read();
        }
        catch (IOException e) {
            throw new IOException("Error reading file: " + location, e);
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);
        ensureOpen();
        try {
            return stream.read(buffer, offset, length);
        }
        catch (IOException e) {
            throw new IOException("Error reading file: " + location, e);
        }
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        long skipSize = clamp(n, 0, fileSize - stream.getPos());
        stream.seek(stream.getPos() + skipSize);
        return skipSize;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();
        if (n <= 0) {
            return;
        }

        long position = stream.getPos() + n;
        if ((position < 0) || (position > fileSize)) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, stream.getPos(), fileSize, location));
        }
        stream.seek(position);
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try {
                stream.close();
            }
            catch (RuntimeException e) {
                throw handleGcsException(e, "closing file", location);
            }
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    private void openStream()
            throws IOException
    {
        this.stream = openGcsInputStream(gcsFileSystem, fileInfo, location);
    }
}
