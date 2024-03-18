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

import io.trino.plugin.varada.dispatcher.warmup.demoter.AcquireResult;
import io.trino.plugin.varada.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.gen.constants.DemoteStatus;

public interface ConnectorSync
{
    int DEFAULT_CATALOG = 0;

    default void setWarmupDemoterService(WarmupDemoterService warmupDemoterService)
    {
        throw new UnsupportedOperationException();
    }

    default int syncDemotePrepare(double epsilon)
    {
        throw new UnsupportedOperationException();
    }

    default void startDemote(int demoteSequence)
    {
        throw new UnsupportedOperationException();
    }

    default void syncDemoteCycleEnd(int demoteSequence, double lowestPriorityExist, double highestPriorityDemoted, DemoteStatus demoteCompleted)
    {
        throw new UnsupportedOperationException();
    }

    default AcquireResult tryAcquireAllocation()
    {
        throw new UnsupportedOperationException();
    }

    default int getCatalogSequence()
    {
        return 0;
    }

    default String getCatalogName()
    {
        return "";
    }
}
