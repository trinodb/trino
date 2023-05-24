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
package io.trino.plugin.hive.line;

import io.trino.filesystem.Location;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineReader;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.OptionalLong;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LinePageSource
        implements ConnectorPageSource
{
    private static final int INSTANCE_SIZE = instanceSize(LinePageSource.class);

    private final LineReader lineReader;
    private final LineDeserializer deserializer;
    private final LineBuffer lineBuffer;
    private final Location filePath;

    private PageBuilder pageBuilder;
    private long completedPositions;

    public LinePageSource(LineReader lineReader, LineDeserializer deserializer, LineBuffer lineBuffer, Location filePath)
    {
        this.lineReader = requireNonNull(lineReader, "lineReader is null");
        this.deserializer = requireNonNull(deserializer, "deserializer is null");
        this.lineBuffer = requireNonNull(lineBuffer, "lineBuffer is null");
        this.filePath = requireNonNull(filePath, "filePath is null");

        this.pageBuilder = new PageBuilder(deserializer.getTypes());
    }

    @Override
    public Page getNextPage()
    {
        try {
            while (!pageBuilder.isFull() && lineReader.readLine(lineBuffer)) {
                deserializer.deserialize(lineBuffer, pageBuilder);
            }
            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            pageBuilder = pageBuilder.newPageBuilderLike();
            return page;
        }
        catch (TrinoException e) {
            closeAllSuppress(e, this);
            throw e;
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, format("Failed to read file at %s", filePath), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        lineReader.close();
    }

    @Override
    public boolean isFinished()
    {
        return lineReader.isClosed();
    }

    @Override
    public long getCompletedBytes()
    {
        return lineReader.getBytesRead();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return lineReader.getReadTimeNanos();
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + lineReader.getRetainedSize() + lineBuffer.getRetainedSize() + pageBuilder.getRetainedSizeInBytes();
    }
}
