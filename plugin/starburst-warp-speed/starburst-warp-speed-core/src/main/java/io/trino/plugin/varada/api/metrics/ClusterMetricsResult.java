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
package io.trino.plugin.varada.api.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ClusterMetricsResult(
        double cpuUsage,
        long memoryAllocated,
        long memoryCapacity,
        long storageAllocated,
        long storageCapacity)
{
    @JsonCreator
    public ClusterMetricsResult(
            @JsonProperty("cpuUsage") double cpuUsage,
            @JsonProperty("memoryAllocated") long memoryAllocated,
            @JsonProperty("memoryCapacity") long memoryCapacity,
            @JsonProperty("storageAllocated") long storageAllocated,
            @JsonProperty("storageCapacity") long storageCapacity)
    {
        this.cpuUsage = cpuUsage;
        this.memoryCapacity = memoryCapacity;
        this.memoryAllocated = memoryAllocated;
        this.storageAllocated = storageAllocated;
        this.storageCapacity = storageCapacity;
    }
}
