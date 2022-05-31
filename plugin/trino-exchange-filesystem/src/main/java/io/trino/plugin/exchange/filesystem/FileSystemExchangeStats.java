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
package io.trino.plugin.exchange.filesystem;

import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class FileSystemExchangeStats
{
    private final ExecutionStats createExchangeSourceHandles = new ExecutionStats();
    private final ExecutionStats getCommittedPartitions = new ExecutionStats();
    private final ExecutionStats closeExchange = new ExecutionStats();
    private final ExecutionStats exchangeSinkBlocked = new ExecutionStats();
    private final ExecutionStats exchangeSinkFinish = new ExecutionStats();
    private final ExecutionStats exchangeSinkAbort = new ExecutionStats();
    private final ExecutionStats exchangeSourceBlocked = new ExecutionStats();
    private final DistributionStat fileSizeInBytes = new DistributionStat();

    @Managed
    @Nested
    public ExecutionStats getCreateExchangeSourceHandles()
    {
        return createExchangeSourceHandles;
    }

    @Managed
    @Nested
    public ExecutionStats getGetCommittedPartitions()
    {
        return getCommittedPartitions;
    }

    @Managed
    @Nested
    public ExecutionStats getCloseExchange()
    {
        return closeExchange;
    }

    @Managed
    @Nested
    public ExecutionStats getExchangeSinkBlocked()
    {
        return exchangeSinkBlocked;
    }

    @Managed
    @Nested
    public ExecutionStats getExchangeSinkFinish()
    {
        return exchangeSinkFinish;
    }

    @Managed
    @Nested
    public ExecutionStats getExchangeSinkAbort()
    {
        return exchangeSinkAbort;
    }

    @Managed
    @Nested
    public ExecutionStats getExchangeSourceBlocked()
    {
        return exchangeSourceBlocked;
    }

    @Managed
    @Nested
    public DistributionStat getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }
}
