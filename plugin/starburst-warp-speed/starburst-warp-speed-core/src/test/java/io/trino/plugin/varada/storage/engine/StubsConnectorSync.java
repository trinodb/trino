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
package io.trino.plugin.varada.storage.engine;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.dispatcher.warmup.demoter.AcquireResult;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.constants.DemoteStatus;
import io.varada.tools.CatalogNameProvider;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Singleton
public class StubsConnectorSync
        implements ConnectorSync
{
    private final CatalogNameProvider catalogNameProvider;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final AtomicInteger demoteSequence = new AtomicInteger();

    private WarmupDemoterService warmupDemoterService;
    private double epsilon;

    @Inject
    public StubsConnectorSync(CatalogNameProvider catalogNameProvider)
    {
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
    }

    @Override
    public void startDemote(int demoteSequence)
    {
        if (this.demoteSequence.get() > 0 && this.demoteSequence.get() != demoteSequence) {
            throw new RuntimeException(
                    format("startDemote::already running with another demote sequence %d -> %d",
                            this.demoteSequence.get(), demoteSequence));
        }
        Future<?> unused = executorService.submit(() -> {
            try {
                warmupDemoterService.connectorSyncStartDemote(demoteSequence);
            }
            catch (Throwable e) {
                // do nothing
            }
        });
    }

    @Override
    public void syncDemoteCycleEnd(int demoteSequence, double lowestPriorityExist, double highestPriorityDemoted, DemoteStatus demoteStatus)
    {
        if (this.demoteSequence.get() > 0 && this.demoteSequence.get() != demoteSequence) {
            throw new RuntimeException(
                    format("syncDemoteCycleEnd::already running with another demote sequence %d -> %d",
                            this.demoteSequence.get(), demoteSequence));
        }

        if (DemoteStatus.DEMOTE_STATUS_REACHED_THRESHOLD.equals(demoteStatus) || DemoteStatus.DEMOTE_STATUS_NO_ELEMENTS_TO_DEMOTE.equals(demoteStatus)) {
            Future<?> unused = executorService.submit(() -> {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    //ignore
                }
                warmupDemoterService.connectorSyncDemoteEnd(demoteSequence, highestPriorityDemoted);
            });
        }
        else {
            Future<?> unused = executorService.submit(() -> {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    //ignore
                }
                warmupDemoterService.connectorSyncStartDemoteCycle(lowestPriorityExist + epsilon, true);
            });
        }
        if (this.demoteSequence.get() > 0) {
            this.demoteSequence.decrementAndGet();
        }
    }

    @Override
    public int syncDemotePrepare(double epsilon)
    {
        if (demoteSequence.get() > 0) {
            throw new RuntimeException(
                    format("syncDemotePrepare::already running with another demote sequence %d",
                            this.demoteSequence.get()));
        }
        this.epsilon = epsilon;
        return demoteSequence.incrementAndGet();
    }

    @Override
    public void setWarmupDemoterService(WarmupDemoterService warmupDemoterService)
    {
        this.warmupDemoterService = warmupDemoterService;
    }

    @Override
    public AcquireResult tryAcquireAllocation()
    {
        return new AcquireResult(1);
    }

    @Override
    public String getCatalogName()
    {
        return catalogNameProvider.get();
    }
}
