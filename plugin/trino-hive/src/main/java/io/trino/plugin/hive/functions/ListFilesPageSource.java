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
package io.trino.plugin.hive.functions;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.functions.ListFilesTableFunction.LIST_FILES_COLUMNS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class ListFilesPageSource
        implements ConnectorPageSource
{
    private static final List<Type> COLUMN_TYPES = LIST_FILES_COLUMNS.stream()
            .map(column -> ((HiveColumnHandle) column).getType())
            .collect(toImmutableList());

    private final FileIterator fileIterator;
    private final TimeZoneKey timeZoneKey;
    private final long readTimeNanos;

    private boolean done;

    public ListFilesPageSource(TrinoFileSystem fileSystem, String path, TimeZoneKey timeZoneKey)
    {
        requireNonNull(fileSystem, "fileSystem is null");
        requireNonNull(path, "path is null");
        requireNonNull(timeZoneKey, "timeZoneKey is null");

        long start = System.nanoTime();
        try {
            fileIterator = fileSystem.listFiles(Location.of(path));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed listing files: " + path, e);
        }
        this.timeZoneKey = timeZoneKey;
        readTimeNanos = System.nanoTime() - start;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
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

        PageBuilder page = new PageBuilder(COLUMN_TYPES);
        try {
            while (fileIterator.hasNext()) {
                FileEntry status = fileIterator.next();
                page.declarePosition();
                BIGINT.writeLong(page.getBlockBuilder(0), packDateTimeWithZone(status.lastModified().toEpochMilli(), timeZoneKey));
                BIGINT.writeLong(page.getBlockBuilder(1), status.length());
                VARCHAR.writeSlice(page.getBlockBuilder(2), utf8Slice(status.location().toString()));
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed listing files", e);
        }
        return page.build();
    }

    @Override
    public void close() {}
}
