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
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.deletionvectors.DeletionVector;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.paimon.PaimonLongUtils.saturatedAdd;
import static java.util.Objects.requireNonNull;

public class PaimonPageSourceWrapper
        implements ConnectorPageSource
{
    private final ConnectorPageSource source;

    private final Optional<DeletionVector> deletionVector;
    private long completedPositions;

    public PaimonPageSourceWrapper(ConnectorPageSource source, Optional<DeletionVector> deletionVector)
    {
        this.source = requireNonNull(source, "source is null");
        this.deletionVector = requireNonNull(deletionVector, "deletionVector is null");
    }

    public static ConnectorPageSource wrap(
            ConnectorPageSource connectorPageSource,
            Optional<DeletionVector> deletionVector)
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
        if (deletionVector.isPresent()) {
            return OptionalLong.of(completedPositions);
        }
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
    public SourcePage getNextSourcePage()
    {
        try {
            OptionalLong startPosition = deletionVector.isPresent() ? OptionalLong.of(startPosition()) : OptionalLong.empty();
            SourcePage next = source.getNextSourcePage();
            if (next == null) {
                return next;
            }
            if (deletionVector.isEmpty()) {
                return next;
            }

            Page page = next.getPage();
            int pageCount = page.getPositionCount();

            Page retained = convertToRetained(page, deletionVector.get(), startPosition.orElseThrow(), pageCount);
            completedPositions = saturatedAdd(
                    completedPositions,
                    retained.getPositionCount(),
                    "retained page position count");
            return SourcePage.create(retained);
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw PaimonPageSourceProvider.wrapPaimonReadException(e);
        }
    }

    private long startPosition()
    {
        return source.getCompletedPositions()
                .orElseThrow(() -> new IllegalStateException(
                        "Deletion-vector page source requires completed positions"));
    }

    @VisibleForTesting
    Page convertToRetained(Page page, DeletionVector deletionVector, long startPosition, int pageCount)
    {
        int[] retained = new int[pageCount];
        int retainedLength = 0;
        for (int pagePosition = 0; pagePosition < pageCount; pagePosition++) {
            if (!deletionVector.isDeleted(deletionVectorRowPosition(startPosition, pagePosition))) {
                retained[retainedLength++] = pagePosition;
            }
        }
        if (retainedLength == pageCount) {
            return page;
        }

        return page.getPositions(retained, 0, retainedLength);
    }

    @VisibleForTesting
    static long deletionVectorRowPosition(long startPosition, int pagePosition)
    {
        try {
            return Math.addExact(startPosition, pagePosition);
        }
        catch (ArithmeticException e) {
            throw new IllegalStateException("Deletion-vector row position overflow for start position %s and page position %s"
                    .formatted(startPosition, pagePosition), e);
        }
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
