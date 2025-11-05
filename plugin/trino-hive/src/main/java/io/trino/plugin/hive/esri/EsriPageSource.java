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
package io.trino.plugin.hive.esri;

import io.trino.filesystem.Location;
import io.trino.hive.formats.esri.EsriReader;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EsriPageSource
        implements ConnectorPageSource
{
    private static final int INSTANCE_SIZE = instanceSize(EsriPageSource.class);

    private final EsriReader esriReader;
    private final Location filePath;

    private final PageBuilder pageBuilder;
    private long completedPositions;
    private boolean finished;

    public EsriPageSource(EsriReader esriReader, List<HiveColumnHandle> columns, Location filePath)
    {
        this.esriReader = requireNonNull(esriReader, "esriReader is null");
        this.filePath = requireNonNull(filePath, "filePath is null");
        requireNonNull(columns, "columns is null");

        pageBuilder = new PageBuilder(columns.stream()
                .map(HiveColumnHandle::getType)
                .collect(toImmutableList()));
    }

    @Override
    public long getCompletedBytes()
    {
        return esriReader.getBytesRead();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return esriReader.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        try {
            while (!pageBuilder.isFull()) {
                if (!esriReader.next(pageBuilder)) {
                    finished = true;
                    break;
                }
            }

            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            pageBuilder.reset();

            return SourcePage.create(page);
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
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
            throws IOException
    {
        esriReader.close();
    }
}
