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
package io.trino.plugin.hive.fs;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.metastore.Column;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCachingDirectoryListerTimeout
{
    private static final Location LOCATION = Location.of("local:///test_table");
    private static final Table TABLE = table();

    @Test
    void testElementTimeoutFailsStuckListing()
            throws IOException
    {
        AtomicInteger listCalls = new AtomicInteger();
        CountDownLatch neverReleased = new CountDownLatch(1);
        TestingFileSystem fileSystem = new TestingFileSystem(listCalls, () -> blockingIterator(neverReleased));
        // only per-element timeout here. the listing hangs on the first element
        CachingDirectoryLister lister = lister(new Duration(0, MILLISECONDS), new Duration(150, MILLISECONDS), 0);

        assertThatThrownBy(() -> drain(lister, fileSystem))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Timed out listing directory");
        assertThat(listCalls).hasValue(1);
        // nothing partial is in cache, so the next listing starts from zero
        assertThat(lister.isCached(LOCATION, TABLE.getSchemaTableName())).isFalse();

        neverReleased.countDown();
        lister.shutdown();
    }

    @Test
    void testTotalTimeoutFailsSlowListing()
            throws IOException
    {
        AtomicInteger listCalls = new AtomicInteger();
        // every element is fast, but the whole listing takes longer than the total timeout
        TestingFileSystem fileSystem = new TestingFileSystem(listCalls, () -> slowIterator(entries(50), 60));
        CachingDirectoryLister lister = lister(new Duration(150, MILLISECONDS), new Duration(0, MILLISECONDS), 0);

        assertThatThrownBy(() -> drain(lister, fileSystem))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Timed out listing directory");
        assertThat(lister.isCached(LOCATION, TABLE.getSchemaTableName())).isFalse();

        lister.shutdown();
    }

    @Test
    void testFreshListingAfterTimeoutSucceedsAndCaches()
            throws IOException
    {
        AtomicInteger listCalls = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        List<FileEntry> entries = entries(3);
        // first listing hangs until we release it. the second listing returns the entries at once
        Supplier<FileIterator> iterators = new Supplier<>()
        {
            private boolean firstCall = true;

            @Override
            public synchronized FileIterator get()
            {
                if (firstCall) {
                    firstCall = false;
                    return blockingIterator(release);
                }
                return fromEntries(entries);
            }
        };
        TestingFileSystem fileSystem = new TestingFileSystem(listCalls, iterators);
        CachingDirectoryLister lister = lister(new Duration(0, MILLISECONDS), new Duration(150, MILLISECONDS), 0);

        assertThatThrownBy(() -> drain(lister, fileSystem))
                .isInstanceOf(TrinoException.class);
        assertThat(lister.isCached(LOCATION, TABLE.getSchemaTableName())).isFalse();

        // retry after the timeout starts a new listing attempt, it works and goes to cache
        assertThat(drain(lister, fileSystem)).hasSize(3);
        assertThat(listCalls).hasValue(2);
        assertThat(lister.isCached(LOCATION, TABLE.getSchemaTableName())).isTrue();

        release.countDown();
        lister.shutdown();
    }

    @Test
    void testConfiguredRetriesRecoverFromTransientFailure()
            throws IOException
    {
        AtomicInteger listCalls = new AtomicInteger();
        List<FileEntry> entries = entries(2);
        // the first two attempts fail, the third one in the same call is ok
        Supplier<FileIterator> iterators = () -> {
            if (listCalls.get() <= 2) {
                return failingIterator();
            }
            return fromEntries(entries);
        };
        TestingFileSystem fileSystem = new TestingFileSystem(listCalls, iterators);
        CachingDirectoryLister lister = lister(new Duration(0, MILLISECONDS), new Duration(0, MILLISECONDS), 2);

        assertThat(drain(lister, fileSystem)).hasSize(2);
        assertThat(listCalls).hasValue(3);
        assertThat(lister.isCached(LOCATION, TABLE.getSchemaTableName())).isTrue();

        lister.shutdown();
    }

    @Test
    void testRetriesExhaustedThrowsAndLeavesCacheClean()
            throws IOException
    {
        AtomicInteger listCalls = new AtomicInteger();
        TestingFileSystem fileSystem = new TestingFileSystem(listCalls, TestCachingDirectoryListerTimeout::failingIterator);
        CachingDirectoryLister lister = lister(new Duration(0, MILLISECONDS), new Duration(0, MILLISECONDS), 2);

        assertThatThrownBy(() -> drain(lister, fileSystem))
                .isInstanceOf(TrinoException.class);
        // one first attempt and two retries
        assertThat(listCalls).hasValue(3);
        assertThat(lister.isCached(LOCATION, TABLE.getSchemaTableName())).isFalse();

        lister.shutdown();
    }

    private static CachingDirectoryLister lister(Duration listingTimeout, Duration elementTimeout, int maxRetries)
    {
        return new CachingDirectoryLister(
                new Duration(5, SECONDS),
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                ImmutableList.of("*"),
                ImmutableList.of(),
                _ -> true,
                listingTimeout,
                elementTimeout,
                maxRetries,
                2,
                1000);
    }

    private static List<TrinoFileStatus> drain(CachingDirectoryLister lister, TrinoFileSystem fileSystem)
            throws IOException
    {
        RemoteIterator<TrinoFileStatus> iterator = lister.listFilesRecursively(fileSystem, TABLE, LOCATION);
        List<TrinoFileStatus> files = new ArrayList<>();
        while (iterator.hasNext()) {
            files.add(iterator.next());
        }
        return files;
    }

    private static List<FileEntry> entries(int count)
    {
        ImmutableList.Builder<FileEntry> builder = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            builder.add(new FileEntry(Location.of("local:///test_table/file" + i), 10, Instant.EPOCH, Optional.empty()));
        }
        return builder.build();
    }

    private static FileIterator fromEntries(List<FileEntry> entries)
    {
        Iterator<FileEntry> iterator = entries.iterator();
        return new FileIterator()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public FileEntry next()
            {
                return iterator.next();
            }
        };
    }

    private static FileIterator slowIterator(List<FileEntry> entries, long perElementDelayMillis)
    {
        Iterator<FileEntry> iterator = entries.iterator();
        return new FileIterator()
        {
            @Override
            public boolean hasNext()
                    throws IOException
            {
                sleep(perElementDelayMillis);
                return iterator.hasNext();
            }

            @Override
            public FileEntry next()
            {
                return iterator.next();
            }
        };
    }

    private static FileIterator blockingIterator(CountDownLatch release)
    {
        return new FileIterator()
        {
            @Override
            public boolean hasNext()
                    throws IOException
            {
                try {
                    release.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", e);
                }
                return false;
            }

            @Override
            public FileEntry next()
            {
                throw new NoSuchElementException();
            }
        };
    }

    private static FileIterator failingIterator()
    {
        return new FileIterator()
        {
            @Override
            public boolean hasNext()
                    throws IOException
            {
                throw new IOException("listing failed");
            }

            @Override
            public FileEntry next()
            {
                throw new NoSuchElementException();
            }
        };
    }

    private static void sleep(long millis)
            throws IOException
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    private static Table table()
    {
        Table.Builder builder = Table.builder();
        builder.getStorageBuilder()
                .setStorageFormat(ORC.toStorageFormat())
                .setLocation(LOCATION.toString());
        return builder
                .setDatabaseName("test_dbname")
                .setOwner(Optional.of("test_owner"))
                .setTableName("test_table")
                .setTableType(MANAGED_TABLE.name())
                .setDataColumns(List.of(new Column("col1", HIVE_STRING, Optional.empty(), Map.of())))
                .setPartitionColumns(List.of())
                .setParameters(Map.of())
                .build();
    }

    private record TestingFileSystem(AtomicInteger listCalls, Supplier<FileIterator> iterators)
            implements TrinoFileSystem
    {
        @Override
        public FileIterator listFiles(Location location)
        {
            listCalls.incrementAndGet();
            return iterators.get();
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
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
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw new UnsupportedOperationException();
        }
    }
}
