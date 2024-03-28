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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;

public class OzoneInputFile
        implements TrinoInputFile
{
    private final OzoneLocation location;
    private final ObjectStore storage;
    private OptionalLong length;
    private Optional<Instant> lastModified = Optional.empty();

    public OzoneInputFile(OzoneLocation location, ObjectStore storage, OptionalLong length)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.length = length;
        location.location().verifyValidFileLocation();
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new OzoneInput(location, storage);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new OzoneTrinoInputStream(location, storage);
    }

    @Override
    public long length()
            throws IOException
    {
        if (length.isEmpty()) {
            loadProperties();
        }
        return length.orElseThrow();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isEmpty()) {
            loadProperties();
        }
        return lastModified.orElseThrow();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        OzoneVolume ozoneVolume = storage.getVolume(location.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(location.bucket());
        try {
            bucket.headObject(location.key());
        }
        catch (OMException e) {
            if (e.getResult().equals(KEY_NOT_FOUND)) {
                return false;
            }
            throw e;
        }
        return true;
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private void loadProperties()
            throws IOException
    {
        OzoneVolume ozoneVolume = storage.getVolume(location.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(location.bucket());
        try {
            OzoneKeyDetails key = bucket.getKey(location.key());
            if (length.isEmpty()) {
                length = OptionalLong.of(key.getDataSize());
            }
            if (lastModified.isEmpty()) {
                lastModified = Optional.of(key.getModificationTime());
            }
        }
        catch (OMException e) {
            if (e.getResult().equals(KEY_NOT_FOUND)) {
                throw new FileNotFoundException(location.toString());
            }
            throw e;
        }
        catch (RuntimeException e) {
            // TODO
            throw new RuntimeException();
        }
    }
}
