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
package io.trino.plugin.paimon;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.deletionvectors.DeletionVector;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Wrap {@link ConnectorPageSource} using deletion vector.
 */
public class PaimonPageSourceWrapper
        implements ConnectorPageSource
{
    private final ConnectorPageSource source;

    private final Optional<DeletionVector> deletionVector;

    public PaimonPageSourceWrapper(
            ConnectorPageSource source, Optional<DeletionVector> deletionVector)
    {
        this.source = source;
        this.deletionVector = deletionVector;
    }

    public static ConnectorPageSource wrap(
            ConnectorPageSource connectorPageSource, Optional<DeletionVector> deletionVector)
    {
        return new PaimonPageSourceWrapper(connectorPageSource, deletionVector);
    }

    @Override
    public long getCompletedBytes()
    {
        return source.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return source.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return source.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        int startPosition = (int) source.getCompletedPositions().orElseThrow();
        Page next = source.getNextPage();
        if (next == null) {
            return next;
        }

        int pageCount = next.getPositionCount();

        return deletionVector
                .map(
                        deletionVector ->
                                convertToRetained(next, deletionVector, startPosition, pageCount))
                .orElse(next);
    }

    @VisibleForTesting
    Page convertToRetained(
            Page page, DeletionVector deletionVector, int startPosition, int pageCount)
    {
        int[] retained = new int[pageCount];
        int retainedLength = 0;
        for (int pagePosition = 0; pagePosition < pageCount; pagePosition++) {
            if (!deletionVector.isDeleted(startPosition + pagePosition)) {
                retained[retainedLength++] = pagePosition;
            }
        }
        if (retainedLength == pageCount) {
            return page;
        }

        return page.getPositions(retained, 0, retainedLength);
    }

    @Override
    public long getMemoryUsage()
    {
        return source.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        source.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return source.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return source.getMetrics();
    }
}
