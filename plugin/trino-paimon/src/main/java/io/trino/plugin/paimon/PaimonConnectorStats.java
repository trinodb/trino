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

import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects runtime metrics for the Paimon connector. Exposed via JMX as
 * {@code paimon.trino.<catalog>.type=PaimonConnectorStats}.
 */
public class PaimonConnectorStats
{
    private final AtomicLong splitCount = new AtomicLong();
    private final AtomicLong writeCommitCount = new AtomicLong();
    private final AtomicLong writeCommitBytes = new AtomicLong();
    private final AtomicLong writeCommitFailures = new AtomicLong();
    private final AtomicLong catalogCacheHits = new AtomicLong();
    private final AtomicLong catalogCacheMisses = new AtomicLong();

    private final DistributionStat splitWeight = new DistributionStat();
    private final DistributionStat splitRowCount = new DistributionStat();
    private final DistributionStat writeCommitTimeNanos = new DistributionStat();
    private final DistributionStat writeBytesPerCommit = new DistributionStat();

    @Managed
    public long getSplitCount()
    {
        return splitCount.get();
    }

    public void incrementSplitCount()
    {
        splitCount.incrementAndGet();
    }

    @Managed
    public long getWriteCommitCount()
    {
        return writeCommitCount.get();
    }

    public void incrementWriteCommitCount()
    {
        writeCommitCount.incrementAndGet();
    }

    @Managed
    public long getWriteCommitBytes()
    {
        return writeCommitBytes.get();
    }

    public void addWriteCommitBytes(long bytes)
    {
        writeCommitBytes.addAndGet(bytes);
    }

    @Managed
    public long getWriteCommitFailures()
    {
        return writeCommitFailures.get();
    }

    public void incrementWriteCommitFailures()
    {
        writeCommitFailures.incrementAndGet();
    }

    @Managed
    public long getCatalogCacheHits()
    {
        return catalogCacheHits.get();
    }

    public void incrementCatalogCacheHits()
    {
        catalogCacheHits.incrementAndGet();
    }

    @Managed
    public long getCatalogCacheMisses()
    {
        return catalogCacheMisses.get();
    }

    public void incrementCatalogCacheMisses()
    {
        catalogCacheMisses.incrementAndGet();
    }

    @Managed
    @Nested
    public DistributionStat getSplitWeight()
    {
        return splitWeight;
    }

    public void addSplitWeight(double weight)
    {
        splitWeight.add((long) (weight * 1000));
    }

    @Managed
    @Nested
    public DistributionStat getSplitRowCount()
    {
        return splitRowCount;
    }

    public void addSplitRowCount(long rowCount)
    {
        splitRowCount.add(rowCount);
    }

    @Managed
    @Nested
    public DistributionStat getWriteCommitTimeNanos()
    {
        return writeCommitTimeNanos;
    }

    public void addWriteCommitTimeNanos(long nanos)
    {
        writeCommitTimeNanos.add(nanos);
    }

    @Managed
    @Nested
    public DistributionStat getWriteBytesPerCommit()
    {
        return writeBytesPerCommit;
    }

    public void addWriteBytesPerCommit(long bytes)
    {
        writeBytesPerCommit.add(bytes);
    }
}
