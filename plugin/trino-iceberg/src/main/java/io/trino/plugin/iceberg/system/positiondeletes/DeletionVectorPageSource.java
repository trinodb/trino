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
package io.trino.plugin.iceberg.system.positiondeletes;

import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class DeletionVectorPageSource
        implements ConnectorPageSource
{
    private final String dataFilePath;
    private final long deletionVectorSize;
    private final List<Long> deletedPositions = new ArrayList<>();
    private final PageBuilder pageBuilder;

    private int currentPosition;
    private long completedPositions;
    private long readTimeNanos;
    private boolean closed;

    public DeletionVectorPageSource(
            TrinoFileSystem fileSystem,
            String dataFilePath,
            String deletionVectorPath,
            long deletionVectorSize,
            long contentOffset,
            int contentSizeInBytes)
    {
        requireNonNull(fileSystem, "fileSystem is null");
        this.dataFilePath = requireNonNull(dataFilePath, "dataFilePath is null");
        this.deletionVectorSize = deletionVectorSize;

        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(deletionVectorPath), deletionVectorSize);
        try (TrinoInput input = inputFile.newInput()) {
            Slice slice = input.readFully(contentOffset, toIntExact(contentSizeInBytes));
            DeletionVector deletionVector = DeletionVector.builder().deserialize(slice).build().orElseThrow();
            deletionVector.forEachDeletedRow(deletedPositions::add);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "Failed to read deletion vector file: " + deletionVectorPath, e);
        }

        pageBuilder = new PageBuilder(List.of(VARCHAR, BIGINT));
    }

    @Override
    public long getCompletedBytes()
    {
        return deletionVectorSize;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed || currentPosition >= deletedPositions.size();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }

        long start = System.nanoTime();
        while (currentPosition < deletedPositions.size() && !pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            long pos = deletedPositions.get(currentPosition++);
            VARCHAR.writeString(pageBuilder.getBlockBuilder(0), dataFilePath);
            BIGINT.writeLong(pageBuilder.getBlockBuilder(1), pos);
        }
        readTimeNanos += System.nanoTime() - start;

        if (!pageBuilder.isEmpty()) {
            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            pageBuilder.reset();
            return SourcePage.create(page);
        }

        close();
        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
