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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.spi.Page;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSortingFileWriter
{
    private static final Location TEMP_DIRECTORY = Location.of("memory:///temp/");

    @Test
    public void testRollbackReleasesMemoryAndDeletesTempFiles()
            throws IOException
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        SortingFileWriter writer = new SortingFileWriter(
                fileSystem,
                TEMP_DIRECTORY.appendPath("sort"),
                new NoOpFileWriter(),
                DataSize.of(1, MEGABYTE),
                100,
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_FIRST),
                PAGE_SORTER,
                new TypeOperators(),
                ImmutableList.of());
        long emptyMemoryUsage = writer.getMemoryUsage();

        for (int i = 0; i < 100; i++) {
            writer.appendRows(createPage(10_000));
        }
        assertThat(writer.getMemoryUsage()).isGreaterThan(emptyMemoryUsage);
        assertThat(listFiles(fileSystem)).isNotEmpty();

        writer.rollback();

        assertThat(writer.getMemoryUsage()).isEqualTo(emptyMemoryUsage);
        assertThat(listFiles(fileSystem)).isEmpty();
    }

    private static Page createPage(int positionCount)
    {
        long[] values = new long[positionCount];
        for (int i = 0; i < positionCount; i++) {
            values[i] = positionCount - i;
        }
        return new Page(new LongArrayBlock(positionCount, Optional.empty(), values));
    }

    private static List<Location> listFiles(TrinoFileSystem fileSystem)
            throws IOException
    {
        ImmutableList.Builder<Location> locations = ImmutableList.builder();
        FileIterator iterator = fileSystem.listFiles(TEMP_DIRECTORY);
        while (iterator.hasNext()) {
            locations.add(iterator.next().location());
        }
        return locations.build();
    }

    private static class NoOpFileWriter
            implements FileWriter
    {
        @Override
        public long getWrittenBytes()
        {
            return 0;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void appendRows(Page dataPage) {}

        @Override
        public RollbackAction commit()
        {
            return () -> {};
        }

        @Override
        public void rollback() {}

        @Override
        public long getValidationCpuNanos()
        {
            return 0;
        }
    }
}
