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
package io.trino.filesystem.ozone;

import io.trino.filesystem.TrinoInput;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;

import java.io.EOFException;
import java.io.IOException;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class OzoneInput
        implements TrinoInput
{
    private final OzoneLocation location;
    private final ObjectStore storage;
    private boolean closed;

    public OzoneInput(OzoneLocation location, ObjectStore storage)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        OzoneVolume ozoneVolume = storage.getVolume(location.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(location.bucket());
        OzoneKeyDetails key = bucket.getKey(location.key());
        long fileSize = key.getDataSize();

        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return;
        }

        try (OzoneInputStream inputStream = bucket.readKey(location.key())) {
            if (position > fileSize) {
                throw new IOException("Cannot read at %s. File size is %s: %s".formatted(position, fileSize, location));
            }
            inputStream.seek(position);
            int readSize = inputStream.read(buffer, bufferOffset, bufferLength);
            if (readSize != bufferLength) {
                throw new EOFException("End of file reached before reading fully: " + location);
            }
        }
        catch (RuntimeException e) {
            // TODO
            throw new RuntimeException();
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        OzoneVolume ozoneVolume = storage.getVolume(location.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(location.bucket());
        OzoneKeyDetails key = bucket.getKey(location.key());
        long fileSize = key.getDataSize();

        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return 0;
        }
        long offset = Math.max(0, fileSize - bufferLength);
        try (OzoneInputStream inputStream = bucket.readKey(location.key())) {
            inputStream.seek(offset);
            int readSize = inputStream.read(buffer, bufferOffset, bufferLength);
            if (readSize != bufferLength) {
                throw new EOFException("End of file reached before reading fully: " + location);
            }
            return readSize;
        }
        catch (RuntimeException e) {
            // TODO
            throw new RuntimeException();
        }
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
}
