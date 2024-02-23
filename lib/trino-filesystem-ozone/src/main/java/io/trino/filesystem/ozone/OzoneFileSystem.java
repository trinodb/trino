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

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public class OzoneFileSystem
        implements TrinoFileSystem
{
    private final ObjectStore objectStore;

    public OzoneFileSystem(ObjectStore objectStore)
    {
        this.objectStore = objectStore;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        throw new UnsupportedOperationException();
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        // is this needed?
        // exist in azure and s3, but not gcs
        location.verifyValidFileLocation();

        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        bucket.deleteKey(ozoneLocation.key());
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        bucket.deleteDirectory(ozoneLocation.key(), true);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        // Ozone does not have directories
        validateOzoneLocation(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new IOException("Ozone does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        validateOzoneLocation(targetPath);
        // Ozone does not have directories
        return Optional.empty();
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateOzoneLocation(Location location)
    {
        new OzoneLocation(location);
    }
}
