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
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.permission.FsPermission.getFileDefault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Test(singleThreaded = true)
public class TestTransactionScopeCachingDirectoryLister
        extends BaseCachingDirectoryListerTest<TransactionScopeCachingDirectoryLister>
{
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"));
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            Optional.of("location"),
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), BUCKETING_V1, 10, ImmutableList.of(new SortingColumn("column", SortingColumn.Order.ASCENDING)))),
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

    @Override
    protected TransactionScopeCachingDirectoryLister createDirectoryLister()
    {
        return new TransactionScopeCachingDirectoryLister(new FileSystemDirectoryLister(), 1_000_000L);
    }

    @Override
    protected boolean isCached(TransactionScopeCachingDirectoryLister directoryLister, Path path)
    {
        return directoryLister.isCached(path);
    }

    @Test
    public void testConcurrentDirectoryListing()
            throws IOException
    {
        LocatedFileStatus firstFile = new LocatedFileStatus(1, false, 1, 1, 1, 1, getFileDefault(), "x", "x", new org.apache.hadoop.fs.Path("x"), new org.apache.hadoop.fs.Path("x"), false, false, false, new BlockLocation[] {});
        LocatedFileStatus secondFile = new LocatedFileStatus(1, false, 1, 1, 1, 1, getFileDefault(), "y", "y", new org.apache.hadoop.fs.Path("y"), new org.apache.hadoop.fs.Path("y"), false, false, false, new BlockLocation[] {});
        LocatedFileStatus thirdFile = new LocatedFileStatus(1, false, 1, 1, 1, 1, getFileDefault(), "z", "z", new org.apache.hadoop.fs.Path("z"), new org.apache.hadoop.fs.Path("z"), false, false, false, new BlockLocation[] {});

        org.apache.hadoop.fs.Path path1 = new org.apache.hadoop.fs.Path("x");
        org.apache.hadoop.fs.Path path2 = new org.apache.hadoop.fs.Path("y");

        CountingDirectoryLister countingLister = new CountingDirectoryLister(
                ImmutableMap.of(
                        path1, ImmutableList.of(firstFile, secondFile),
                        path2, ImmutableList.of(thirdFile)));

        TransactionScopeCachingDirectoryLister cachingLister = new TransactionScopeCachingDirectoryLister(countingLister, 2);

        assertFiles(cachingLister.list(null, TABLE, path2), ImmutableList.of(thirdFile));
        assertThat(countingLister.getListCount()).isEqualTo(1);

        // listing path2 again shouldn't increase listing count
        assertThat(cachingLister.isCached(path2)).isTrue();
        assertFiles(cachingLister.list(null, TABLE, path2), ImmutableList.of(thirdFile));
        assertThat(countingLister.getListCount()).isEqualTo(1);

        // start listing path1 concurrently
        RemoteIterator<LocatedFileStatus> path1FilesA = cachingLister.list(null, TABLE, path1);
        RemoteIterator<LocatedFileStatus> path1FilesB = cachingLister.list(null, TABLE, path1);
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
        assertThat(cachingLister.isCached(path2)).isFalse();
        assertFiles(cachingLister.list(null, TABLE, path2), ImmutableList.of(thirdFile));
        assertThat(countingLister.getListCount()).isEqualTo(3);
    }

    @Test
    public void testConcurrentDirectoryListingException()
            throws IOException
    {
        LocatedFileStatus file = new LocatedFileStatus(1, false, 1, 1, 1, 1, getFileDefault(), "x", "x", new org.apache.hadoop.fs.Path("x"), new org.apache.hadoop.fs.Path("x"), false, false, false, new BlockLocation[] {});
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("x");

        CountingDirectoryLister countingLister = new CountingDirectoryLister(ImmutableMap.of(path, ImmutableList.of(file)));
        DirectoryLister cachingLister = new TransactionScopeCachingDirectoryLister(countingLister, 1);

        // start listing path concurrently
        countingLister.setThrowException(true);
        RemoteIterator<LocatedFileStatus> filesA = cachingLister.list(null, TABLE, path);
        RemoteIterator<LocatedFileStatus> filesB = cachingLister.list(null, TABLE, path);
        assertThat(countingLister.getListCount()).isEqualTo(1);

        // listing should throw an exception
        assertThatThrownBy(filesA::hasNext).isInstanceOf(IOException.class);

        // listing again should succeed
        countingLister.setThrowException(false);
        assertFiles(cachingLister.list(null, TABLE, path), ImmutableList.of(file));
        assertThat(countingLister.getListCount()).isEqualTo(2);

        // listing using second concurrently initialized DirectoryLister should fail
        assertThatThrownBy(filesB::hasNext).isInstanceOf(IOException.class);
    }

    private void assertFiles(RemoteIterator<LocatedFileStatus> iterator, List<LocatedFileStatus> expectedFiles)
            throws IOException
    {
        ImmutableList.Builder<LocatedFileStatus> actualFiles = ImmutableList.builder();
        while (iterator.hasNext()) {
            actualFiles.add(iterator.next());
        }
        assertThat(actualFiles.build()).isEqualTo(expectedFiles);
    }

    private static class CountingDirectoryLister
            implements DirectoryLister
    {
        private final Map<org.apache.hadoop.fs.Path, List<LocatedFileStatus>> fileStatuses;
        private int listCount;
        private boolean throwException;

        public CountingDirectoryLister(Map<org.apache.hadoop.fs.Path, List<LocatedFileStatus>> fileStatuses)
        {
            this.fileStatuses = requireNonNull(fileStatuses, "fileStatuses is null");
        }

        @Override
        public RemoteIterator<LocatedFileStatus> list(FileSystem fs, Table table, org.apache.hadoop.fs.Path path)
                throws IOException
        {
            listCount++;
            return throwingRemoteIterator(requireNonNull(fileStatuses.get(path)), throwException);
        }

        public void setThrowException(boolean throwException)
        {
            this.throwException = throwException;
        }

        public int getListCount()
        {
            return listCount;
        }

        @Override
        public void invalidate(Partition partition)
        {
        }

        @Override
        public void invalidate(Table table)
        {
        }
    }

    static RemoteIterator<LocatedFileStatus> throwingRemoteIterator(List<LocatedFileStatus> files, boolean throwException)
    {
        return new RemoteIterator<>()
        {
            private final Iterator<LocatedFileStatus> iterator = ImmutableList.copyOf(files).iterator();

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
            public LocatedFileStatus next()
            {
                return iterator.next();
            }
        };
    }
}
