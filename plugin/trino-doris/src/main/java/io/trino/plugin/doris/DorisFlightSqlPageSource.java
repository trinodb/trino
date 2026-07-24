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
package io.trino.plugin.doris;

import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class DorisFlightSqlPageSource
        implements ConnectorPageSource
{
    private final DorisFlightSqlResult result;
    private final DorisArrowToPageConverter converter;
    private final List<DorisColumnHandle> columns;
    private final PageBuilder pageBuilder;

    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private boolean finished;

    public DorisFlightSqlPageSource(DorisFlightSqlResult result, DorisArrowToPageConverter converter, List<DorisColumnHandle> columns)
    {
        this.result = requireNonNull(result, "result is null");
        this.converter = requireNonNull(converter, "converter is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.pageBuilder = new PageBuilder(this.columns.stream()
                .map(DorisColumnHandle::columnType)
                .toList());
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
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
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        LoadedPage currentPage = loadNextPage();
        if (currentPage == null) {
            finished = true;
            return null;
        }

        completedPositions += currentPage.positionCount();
        completedBytes += currentPage.completedBytes();
        readTimeNanos += currentPage.readTimeNanos();
        return currentPage.sourcePage();
    }

    @Override
    public long getMemoryUsage()
    {
        return result.getMemoryUsage() + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        finished = true;
        result.close();
    }

    private LoadedPage loadNextPage()
    {
        while (true) {
            long start = System.nanoTime();
            boolean hasBatch = result.loadNextBatch();
            long batchReadTimeNanos = System.nanoTime() - start;

            if (!hasBatch) {
                return null;
            }

            VectorSchemaRoot root = result.getVectorSchemaRoot();
            if (root.getRowCount() == 0) {
                readTimeNanos += batchReadTimeNanos;
                continue;
            }

            long batchBytes = root.getFieldVectors().stream()
                    .mapToLong(FieldVector::getBufferSize)
                    .sum();
            return new LoadedPage(
                    SourcePage.create(converter.convert(pageBuilder, columns, root)),
                    root.getRowCount(),
                    batchBytes,
                    batchReadTimeNanos);
        }
    }

    private record LoadedPage(SourcePage sourcePage, int positionCount, long completedBytes, long readTimeNanos) {}
}
