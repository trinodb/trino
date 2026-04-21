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
package io.trino.plugin.resourcegroups.db;

import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResourceGroupGlobalProperties
{
    private final Optional<Duration> cpuQuotaPeriod;
    private final Optional<Duration> physicalDataScanQuotaPeriod;

    private ResourceGroupGlobalProperties(Optional<Duration> cpuQuotaPeriod, Optional<Duration> physicalDataScanQuotaPeriod)
    {
        this.cpuQuotaPeriod = requireNonNull(cpuQuotaPeriod, "cpuQuotaPeriod is null");
        this.physicalDataScanQuotaPeriod = requireNonNull(physicalDataScanQuotaPeriod, "physicalDataScanQuotaPeriod is null");
    }

    public Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod;
    }

    public Optional<Duration> getPhysicalDataScanQuotaPeriod()
    {
        return physicalDataScanQuotaPeriod;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof ResourceGroupGlobalProperties that)) {
            return false;
        }
        return cpuQuotaPeriod.equals(that.cpuQuotaPeriod) && physicalDataScanQuotaPeriod.equals(that.physicalDataScanQuotaPeriod);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuQuotaPeriod, physicalDataScanQuotaPeriod);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<Duration> cpuQuotaPeriod = Optional.empty();
        private Optional<Duration> physicalDataScanQuotaPeriod = Optional.empty();

        public Builder setCpuQuotaPeriod(Optional<Duration> cpuQuotaPeriod)
        {
            this.cpuQuotaPeriod = cpuQuotaPeriod;
            return this;
        }

        public Builder setPhysicalDataScanQuotaPeriod(Optional<Duration> physicalDataScanQuotaPeriod)
        {
            this.physicalDataScanQuotaPeriod = physicalDataScanQuotaPeriod;
            return this;
        }

        public ResourceGroupGlobalProperties build()
        {
            return new ResourceGroupGlobalProperties(cpuQuotaPeriod, physicalDataScanQuotaPeriod);
        }
    }
}
