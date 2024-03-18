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
package io.trino.plugin.varada.metrics.persistent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.execution.TaskData;
import io.trino.plugin.varada.metrics.VaradaStatsBase;

import java.util.Collection;

public class PersistentMetricData
        extends TaskData
{
    // copy from CoordinatorPersistentMetricUpdater
    public static final String TASK_NAME = "update-persistent-metrics";

    private final Collection<VaradaStatsBase> persistentMetricsData;

    @JsonCreator
    public PersistentMetricData(@JsonProperty("metricsData") Collection<VaradaStatsBase> persistentMetricsData)
    {
        this.persistentMetricsData = persistentMetricsData;
    }

    @JsonProperty
    public Collection<VaradaStatsBase> getPersistentMetricsData()
    {
        return persistentMetricsData;
    }

    @Override
    protected String getTaskName()
    {
        return TASK_NAME;
    }
}
