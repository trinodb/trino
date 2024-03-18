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
package io.varada.cloudstorage;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.varada.cloudstorage.hdfs.HdfsCloudStorage;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SwitchingCloudStorage
        implements CloudStorage
{
    private final Optional<HdfsCloudStorage> hdfsCloudStorage;
    private final Map<String, CloudStorage> cloudStorageMap;

    public SwitchingCloudStorage(Optional<HdfsCloudStorage> hdfsCloudStorage, Map<String, CloudStorage> cloudStorageMap)
    {
        this.hdfsCloudStorage = requireNonNull(hdfsCloudStorage, "hdfsCloudStorage is null");
        this.cloudStorageMap = ImmutableMap.copyOf(requireNonNull(cloudStorageMap, "cloudStorageMap is null"));
    }

    private CloudStorage cloudStorage(Location location)
    {
        return location.scheme()
                .map(cloudStorageMap::get)
                .or(() -> hdfsCloudStorage)
                .orElseThrow(() -> new IllegalArgumentException("No cloudStorage for location: " + location));
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return cloudStorage(location).newInputFile(location);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return cloudStorage(location).newOutputFile(location);
    }

    @Override
    public void uploadFile(Location source, Location target)
            throws IOException
    {
        cloudStorage(target).uploadFile(source, target);
    }

    @Override
    public void downloadFile(Location source, Location target)
            throws IOException
    {
        cloudStorage(source).downloadFile(source, target);
    }

    @Override
    public void copyFile(Location source, Location destination)
            throws IOException
    {
        cloudStorage(source).copyFile(source, destination);
    }

    @Override
    public void copyFileReplaceTail(Location source, Location destination, long position, byte[] tailBuffer)
            throws IOException
    {
        cloudStorage(source).copyFileReplaceTail(source, destination, position, tailBuffer);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        cloudStorage(location).deleteFile(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        cloudStorage(source).renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return cloudStorage(location).listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return cloudStorage(location).directoryExists(location);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return cloudStorage(location).listDirectories(location);
    }
}
