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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Execution(SAME_THREAD)
public class TestTransactionScopeCachingDirectoryLister
{
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"),
            Map.of());
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            Optional.of("location"),
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), 10, ImmutableList.of(new SortingColumn("column", SortingColumn.Order.ASCENDING)))),
            true,
            ImmutableMap.of("param", "value2"));
    private static final Table TABLE = new Table(
            "database",
            "table",
            Optional.of("owner"),
            "table_type",
            TABLE_STORAGE,
            ImmutableList.of(TABLE_COLUMN),
            ImmutableList.of(TABLE_COLUMN),
            ImmutableMap.of("param", "value3"),
            Optional.of("original_text"),
            Optional.of("expanded_text"),
            OptionalLong.empty());

    @Test
    public void testConcurrentDirectoryListing()
            throws IOException
    {
        TrinoFileStatus firstFile = new TrinoFileStatus(ImmutableList.of(), "file:/x/x", false, 1, 1);
        TrinoFileStatus secondFile = new TrinoFileStatus(ImmutableList.of(), "file:/x/y", false, 1, 1);
        TrinoFileStatus thirdFile = new TrinoFileStatus(ImmutableList.of(), "file:/y/z", false, 1, 1);

        Location path1 = Location.of("file:/x");
        Location path2 = Location.of("file:/y");

        CountingDirectoryLister countingLister = new CountingDirectoryLister(
                ImmutableMap.of(
                        path1, ImmutableList.of(firstFile, secondFile),
                        path2, ImmutableList.of(thirdFile)));

        // Set concurrencyLevel to 1 as EvictableCache with higher concurrencyLimit is not deterministic
        // due to Token being a key in segmented cache.
        TransactionScopeCachingDirectoryLister cachingLister = (TransactionScopeCachingDirectoryLister) new TransactionScopeCachingDirectoryListerFactory(DataSize.ofBytes(500), Optional.of(1)).get(countingLister);

        assertFiles(new DirectoryListingFilter(path2, cachingLister.listFilesRecursively(null, TABLE, path2), true), ImmutableList.of(thirdFile));
        assertThat(countingLister.getListCount()).isEqualTo(1);

        // listing path2 again shouldn't increase listing count
        assertThat(cachingLister.isCached(path2, TABLE.getSchemaTableName())).isTrue();
        assertFiles(new DirectoryListingFilter(path2, cachingLister.listFilesRecursively(null, TABLE, path2), true), ImmutableList.of(thirdFile));
        assertThat(countingLister.getListCount()).isEqualTo(1);

        // start listing path1 concurrently
        RemoteIterator<TrinoFileStatus> path1FilesA = new DirectoryListingFilter(path1, cachingLister.listFilesRecursively(null, TABLE, path1), true);
        RemoteIterator<TrinoFileStatus> path1FilesB = new DirectoryListingFilter(path1, cachingLister.listFilesRecursively(null, TABLE, path1), true);
        assertThat(countingLister.getListCount()).isEqualTo(2);

        // list path1 files using both iterators concurrently
        assertThat(path1FilesA.next()).isEqualTo(firstFile);
        assertThat(path1FilesB.next()).isEqualTo(firstFile);
        assertThat(path1FilesB.next()).isEqualTo(secondFile);
        assertThat(path1FilesA.next()).isEqualTo(secondFile);
        assertThat(path1FilesA.hasNext()).isFalse();
        assertThat(path1FilesB.hasNext()).isFalse();
        assertThat(countingLister.getListCount()).isEqualTo(2);

        // listing path2 again should increase listing count because 2 files were cached for path1
        assertThat(cachingLister.isCached(path2, TABLE.getSchemaTableName())).isFalse();
        assertFiles(new DirectoryListingFilter(path2, cachingLister.listFilesRecursively(null, TABLE, path2), true), ImmutableList.of(thirdFile));
        assertThat(countingLister.getListCount()).isEqualTo(3);
    }

    @Test
    public void testConcurrentDirectoryListingException()
            throws IOException
    {
        TrinoFileStatus file = new TrinoFileStatus(ImmutableList.of(), "file:/x/x", false, 1, 1);
        Location path = Location.of("file:/x");

        CountingDirectoryLister countingLister = new CountingDirectoryLister(ImmutableMap.of(path, ImmutableList.of(file)));
        // Set concurrencyLevel to 1 to ensure deterministic behavior
        DirectoryLister cachingLister = new TransactionScopeCachingDirectoryListerFactory(DataSize.ofBytes(600), Optional.of(1)).get(countingLister);

        // start listing path concurrently
        countingLister.setThrowException(true);
        RemoteIterator<TrinoFileStatus> filesA = cachingLister.listFilesRecursively(null, TABLE, path);
        RemoteIterator<TrinoFileStatus> filesB = cachingLister.listFilesRecursively(null, TABLE, path);
        assertThat(countingLister.getListCount()).isEqualTo(1);

        // listing should throw an exception
        assertThatThrownBy(filesA::hasNext).isInstanceOf(IOException.class);

        // listing again should succeed
        countingLister.setThrowException(false);
        assertFiles(new DirectoryListingFilter(path, cachingLister.listFilesRecursively(null, TABLE, path), true), ImmutableList.of(file));
        assertThat(countingLister.getListCount()).isEqualTo(2);

        // listing using second concurrently initialized DirectoryLister should fail
        assertThatThrownBy(filesB::hasNext).isInstanceOf(IOException.class);
    }

    private void assertFiles(RemoteIterator<TrinoFileStatus> iterator, List<TrinoFileStatus> expectedFiles)
            throws IOException
    {
        ImmutableList.Builder<TrinoFileStatus> actualFiles = ImmutableList.builder();
        while (iterator.hasNext()) {
            actualFiles.add(iterator.next());
        }
        assertThat(actualFiles.build()).isEqualTo(expectedFiles);
    }

    private static class CountingDirectoryLister
            implements DirectoryLister
    {
        private final Map<Location, List<TrinoFileStatus>> fileStatuses;
        private final AtomicInteger listCount = new AtomicInteger();
        private volatile boolean throwException;

        public CountingDirectoryLister(Map<Location, List<TrinoFileStatus>> fileStatuses)
        {
            this.fileStatuses = requireNonNull(fileStatuses, "fileStatuses is null");
        }

        @Override
        public RemoteIterator<TrinoFileStatus> listFilesRecursively(TrinoFileSystem fs, Table table, Location location)
        {
            // No specific recursive files-only listing implementation
            listCount.incrementAndGet();
            return throwingRemoteIterator(requireNonNull(fileStatuses.get(location)), throwException);
        }

        public void setThrowException(boolean throwException)
        {
            this.throwException = throwException;
        }

        public int getListCount()
        {
            return listCount.get();
        }

        @Override
        public void invalidate(Location location, SchemaTableName schemaTableName) {}

        @Override
        public void invalidate(Partition partition) {}

        @Override
        public void invalidate(Table table) {}

        @Override
        public void invalidateAll() {}
    }

    static RemoteIterator<TrinoFileStatus> throwingRemoteIterator(List<TrinoFileStatus> files, boolean throwException)
    {
        return new RemoteIterator<>()
        {
            private final Iterator<TrinoFileStatus> iterator = ImmutableList.copyOf(files).iterator();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                if (throwException) {
                    throw new IOException();
                }
                return iterator.hasNext();
            }

            @Override
            public TrinoFileStatus next()
            {
                return iterator.next();
            }
        };
    }
}
