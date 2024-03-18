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
package io.trino.plugin.varada.dispatcher.warmup;

import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmingService;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class WarpCacheTask
        implements WorkerSubmittableTask
{
    private final CountDownLatch taskStartedLatch = new CountDownLatch(1);
    private final CountDownLatch pageSinkFinishLatch = new CountDownLatch(1);

    private final VaradaStatsWarmingService statsWarmingService;

    private final RowGroupKey rowGroupKey;
    private final UUID id;

    public WarpCacheTask(VaradaStatsWarmingService statsWarmingService,
            RowGroupKey rowGroupKey)
    {
        this.statsWarmingService = statsWarmingService;
        this.rowGroupKey = rowGroupKey;
        this.id = UUID.randomUUID();
    }

    @Override
    public UUID getId()
    {
        return id;
    }

    @Override
    public int getPriority()
    {
        return WorkerTaskExecutorService.TaskExecutionType.CACHE.getPriority();
    }

    @Override
    public RowGroupKey getRowGroupKey()
    {
        return rowGroupKey;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmingService.incwarm_scheduled();
    }

    @Override
    public void run()
    {
        taskStartedLatch.countDown();
        try {
            //do warm
            pageSinkFinishLatch.await();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public CountDownLatch getTaskStartedLatch()
    {
        return taskStartedLatch;
    }

    public CountDownLatch getPageSinkFinishLatch()
    {
        return pageSinkFinishLatch;
    }
}
