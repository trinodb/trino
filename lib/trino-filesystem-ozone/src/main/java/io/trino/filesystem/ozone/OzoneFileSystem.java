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
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;

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
        return new OzoneInputFile(ozoneLocation, objectStore, OptionalLong.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        return new OzoneInputFile(ozoneLocation, objectStore, OptionalLong.of(length));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        return new OzoneOutputFile(ozoneLocation, objectStore);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        // is this needed?
        // exist in azure and s3, but not gcs
         location.verifyValidFileLocation();
        // also, blob storage should allow tailing '/' in filename? or is it not supported?

        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        try {
            bucket.deleteKey(ozoneLocation.key());
        }
        catch (OMException e) {
            if (e.getResult().equals(KEY_NOT_FOUND)) {
                return;
            }
            throw e;
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());

        String key = ozoneLocation.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        Iterator<? extends OzoneKey> iterator = bucket.listKeys(key);
        List<String> keyList = new ArrayList<>();
        iterator.forEachRemaining(f -> keyList.add(f.getName()));
        bucket.deleteKeys(keyList);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        // blob storage doesn't need to implement this
        throw new IOException("Ozone does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        return new OzoneFileIterator(ozoneLocation, bucket.listKeys(ozoneLocation.key()));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        // TODO: is listFiles() needed?
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
        // Ozone with File System Optimization should support this op, with the cost of some performance penalty
        // Object storage style access should not implement this
        // See: https://ozone.apache.org/docs/current/feature/prefixfso.html
        throw new IOException("Ozone does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        // TODO: is this needed for blob storage?
        // TOOD: handle volume and bucket listing
        // see org.apache.hadoop.fs.ozone.BasicRootedOzoneClientAdapterImpl#listStatus
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        // TODO: int OZONE_FS_LISTING_PAGE_SIZE_DEFAULT = 1024;
        // TODO: isPartialPrefix?
        return bucket.listStatus(ozoneLocation.key(), false, "", 1024)
                .stream()
                .filter(OzoneFileStatus::isDirectory)
                .map(OzoneFileStatus::getPath)
                .map(baseLocation::appendPath)
                .map(Location::of)
                .collect(toImmutableSet());
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
