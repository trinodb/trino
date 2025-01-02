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
package io.trino.plugin.storage.functions;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.storage.StorageColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ListFilesPageSource
        implements ConnectorPageSource
{
    private final List<? extends ColumnHandle> columns;
    private final long readTimeNanos;
    private final FileIterator fileStatuses;
    private boolean done;

    public ListFilesPageSource(TrinoFileSystem fileSystem, String location, List<? extends ColumnHandle> columns)
    {
        requireNonNull(fileSystem, "fileSystem is null");
        requireNonNull(location, "location is null");
        requireNonNull(columns, "columns is null");

        long start = System.nanoTime();
        try {
            this.fileStatuses = fileSystem.listFiles(Location.of(location));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        readTimeNanos = System.nanoTime() - start;
        this.columns = ImmutableList.copyOf(columns);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return done;
    }

    @Override
    public Page getNextPage()
    {
        if (done) {
            return null;
        }

        done = true;

        PageBuilder page = new PageBuilder(columns.stream().map(column -> ((StorageColumnHandle) column).type()).toList());
        try {
            while (fileStatuses.hasNext()) {
                FileEntry status = fileStatuses.next();
                page.declarePosition();
                for (int i = 0; i < columns.size(); i++) {
                    StorageColumnHandle column = (StorageColumnHandle) columns.get(i);
                    switch (column.name()) {
                        case "file_modified_time" -> BIGINT.writeLong(page.getBlockBuilder(i), packDateTimeWithZone(status.lastModified().toEpochMilli(), UTC_KEY));
                        case "size" -> BIGINT.writeLong(page.getBlockBuilder(i), status.length());
                        case "name" -> VARCHAR.writeSlice(page.getBlockBuilder(i), Slices.utf8Slice(status.location().toString()));
                        default -> throw new IllegalStateException("Unknown column name " + column.name());
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return page.build();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
