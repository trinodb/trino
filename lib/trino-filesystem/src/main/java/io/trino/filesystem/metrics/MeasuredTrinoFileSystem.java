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
package io.trino.filesystem.metrics;

import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.plugin.base.metrics.ApiCallStats;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MeasuredTrinoFileSystem
        implements TrinoFileSystem
{
    private final TrinoFileSystem delegate;
    private final ApiCallStats apiCallStats;

    public MeasuredTrinoFileSystem(TrinoFileSystem delegate, ApiCallStats apiCallStats)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.apiCallStats = requireNonNull(apiCallStats, "apiCallStats is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return apiCallStats.measure("newInputFile", () -> delegate.newInputFile(location));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return apiCallStats.measure("newEncryptedInputFile", () -> delegate.newEncryptedInputFile(location, key));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return apiCallStats.measure("newInputFile", () -> delegate.newInputFile(location, length));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return apiCallStats.measure("newEncryptedInputFile", () -> delegate.newEncryptedInputFile(location, length, key));
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return apiCallStats.measure("newInputFile", () -> delegate.newInputFile(location, length, lastModified));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return apiCallStats.measure("newEncryptedInputFile", () -> delegate.newEncryptedInputFile(location, length, lastModified, key));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return apiCallStats.measure("newOutputFile", () -> delegate.newOutputFile(location));
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return apiCallStats.measure("newEncryptedOutputFile", () -> delegate.newEncryptedOutputFile(location, key));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        measure("deleteFile", () -> {
            delegate.deleteFile(location);
            return null;
        });
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        measure("deleteFiles", () -> {
            delegate.deleteFiles(locations);
            return null;
        });
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        measure("deleteDirectory", () -> {
            delegate.deleteDirectory(location);
            return null;
        });
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        measure("renameFile", () -> {
            delegate.renameFile(source, target);
            return null;
        });
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return measure("listFiles", () -> delegate.listFiles(location));
    }

    @Override
    public FileIterator listFilesStartingFrom(Location location, String startingFrom)
            throws IOException
    {
        return measure("listFilesStartingFrom", () -> delegate.listFilesStartingFrom(location, startingFrom));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return measure("directoryExists", () -> delegate.directoryExists(location));
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        measure("createDirectory", () -> {
            delegate.createDirectory(location);
            return null;
        });
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        measure("renameDirectory", () -> {
            delegate.renameDirectory(source, target);
            return null;
        });
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return measure("listDirectories", () -> delegate.listDirectories(location));
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return measure("createTemporaryDirectory", () -> delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix));
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return measure("preSignedUri", () -> delegate.preSignedUri(location, ttl));
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return measure("encryptedPreSignedUri", () -> delegate.encryptedPreSignedUri(location, ttl, key));
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics().mergeWith(apiCallStats.toMetrics("filesystem"));
    }

    private <T> T measure(String callName, Call<T> call)
            throws IOException
    {
        try {
            return apiCallStats.measure(callName, () -> {
                try {
                    return call.call();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private interface Call<T>
    {
        T call()
                throws IOException;
    }
}
