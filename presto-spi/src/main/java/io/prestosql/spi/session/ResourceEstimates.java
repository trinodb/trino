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
package io.prestosql.spi.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Estimated resource usage for a query.
 * <p>
 * This class is under active development and should be considered beta.
 */
public final class ResourceEstimates
{
    public static final String EXECUTION_TIME = "EXECUTION_TIME";
    public static final String CPU_TIME = "CPU_TIME";
    public static final String PEAK_MEMORY = "PEAK_MEMORY";

    private final Optional<Duration> executionTime;
    private final Optional<Duration> cpuTime;
    private final Optional<Long> peakMemoryBytes;

    @JsonCreator
    public ResourceEstimates(
            @JsonProperty("executionTime") Optional<Duration> executionTime,
            @JsonProperty("cpuTime") Optional<Duration> cpuTime,
            @JsonProperty("peakMemoryBytes") Optional<Long> peakMemoryBytes)
    {
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.peakMemoryBytes = requireNonNull(peakMemoryBytes, "peakMemoryBytes is null");
    }

    @JsonProperty
    public Optional<Duration> getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public Optional<Duration> getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public Optional<Long> getPeakMemoryBytes()
    {
        return peakMemoryBytes;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("ResourceEstimates{");
        sb.append("executionTime=").append(executionTime);
        sb.append(", cpuTime=").append(cpuTime);
        sb.append(", peakMemoryBytes=").append(peakMemoryBytes);
        sb.append('}');
        return sb.toString();
    }
}
