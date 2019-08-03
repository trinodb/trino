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
package io.prestosql.execution.scheduler;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class TopologyAwareNodeSelectorConfig
{
    private List<String> locationSegmentNames = ImmutableList.of("machine");
    private String networkTopologyFile = "";
    private Duration refreshPeriod = new Duration(12, HOURS);

    @NotNull
    public List<String> getLocationSegmentNames()
    {
        return locationSegmentNames;
    }

    public TopologyAwareNodeSelectorConfig setLocationSegmentNames(List<String> locationSegmentNames)
    {
        this.locationSegmentNames = requireNonNull(locationSegmentNames, "locationSegmentNames is null");
        return this;
    }

    @Config("node-scheduler.network-topology-segments")
    public TopologyAwareNodeSelectorConfig setLocationSegmentNames(String locationSegmentNames)
    {
        this.locationSegmentNames = Splitter.on(",")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(locationSegmentNames);
        return this;
    }

    public String getNetworkTopologyFile()
    {
        return networkTopologyFile;
    }

    @Config("node-scheduler.network-topology-file")
    public TopologyAwareNodeSelectorConfig setNetworkTopologyFile(String networkTopologyFile)
    {
        this.networkTopologyFile = networkTopologyFile;
        return this;
    }

    @MinDuration("1ms")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config("node-scheduler.refresh-period")
    public TopologyAwareNodeSelectorConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }
}
