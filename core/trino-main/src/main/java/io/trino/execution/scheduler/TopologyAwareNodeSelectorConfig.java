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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class TopologyAwareNodeSelectorConfig
{
    public enum TopologyType
    {
        FLAT, FILE, SUBNET
    }

    private TopologyType type = TopologyType.FLAT;
    private List<String> locationSegmentNames = ImmutableList.of("machine");

    @NotNull
    public TopologyType getType()
    {
        return type;
    }

    @Config("node-scheduler.network-topology.type")
    public TopologyAwareNodeSelectorConfig setType(TopologyType type)
    {
        this.type = type;
        return this;
    }

    @NotNull
    public List<String> getLocationSegmentNames()
    {
        return locationSegmentNames;
    }

    @Config("node-scheduler.network-topology.segments")
    public TopologyAwareNodeSelectorConfig setLocationSegmentNames(List<String> locationSegmentNames)
    {
        this.locationSegmentNames = ImmutableList.copyOf(locationSegmentNames);
        return this;
    }
}
