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
package io.trino.execution.resourcegroups;

import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedSubtract;

@Immutable
final class ResourceUsage
{
    private final long cpuUsageMillis;
    private final long memoryUsageBytes;
    private final long physicalInputDataUsageBytes;

    public ResourceUsage(long cpuUsageMillis, long memoryUsageBytes, long physicalInputDataUsageBytes)
    {
        this.cpuUsageMillis = cpuUsageMillis;
        this.memoryUsageBytes = memoryUsageBytes;
        this.physicalInputDataUsageBytes = physicalInputDataUsageBytes;
    }

    public ResourceUsage add(ResourceUsage other)
    {
        long newCpuUsageMillis = saturatedAdd(this.cpuUsageMillis, other.cpuUsageMillis);
        long newMemoryUsageBytes = saturatedAdd(this.memoryUsageBytes, other.memoryUsageBytes);
        long newPhysicalInputDataUsageBytes = saturatedAdd(this.physicalInputDataUsageBytes, other.physicalInputDataUsageBytes);
        return new ResourceUsage(newCpuUsageMillis, newMemoryUsageBytes, newPhysicalInputDataUsageBytes);
    }

    public ResourceUsage subtract(ResourceUsage other)
    {
        long newCpuUsageMillis = saturatedSubtract(this.cpuUsageMillis, other.cpuUsageMillis);
        long newMemoryUsageBytes = saturatedSubtract(this.memoryUsageBytes, other.memoryUsageBytes);
        long newPhysicalInputDataUsageBytes = saturatedSubtract(this.physicalInputDataUsageBytes, other.physicalInputDataUsageBytes);
        return new ResourceUsage(newCpuUsageMillis, newMemoryUsageBytes, newPhysicalInputDataUsageBytes);
    }

    public long getCpuUsageMillis()
    {
        return cpuUsageMillis;
    }

    public long getMemoryUsageBytes()
    {
        return memoryUsageBytes;
    }

    public long getPhysicalInputDataUsageBytes()
    {
        return physicalInputDataUsageBytes;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if ((other == null) || (getClass() != other.getClass())) {
            return false;
        }

        ResourceUsage otherUsage = (ResourceUsage) other;
        return cpuUsageMillis == otherUsage.cpuUsageMillis
                && memoryUsageBytes == otherUsage.memoryUsageBytes
                && physicalInputDataUsageBytes == otherUsage.physicalInputDataUsageBytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuUsageMillis, memoryUsageBytes, physicalInputDataUsageBytes);
    }
}
