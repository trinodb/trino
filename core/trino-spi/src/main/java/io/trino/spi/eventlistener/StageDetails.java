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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public class StageDetails
{
    private final String id;
    private final String state;
    private final List<TaskDetails> tasks;
    private final boolean fullyBlocked;
    private final BigDecimal scheduledTime;
    private final BigDecimal blockedTime;
    private final BigDecimal cpuTime;
    private final long inputBuffer;

    @JsonCreator
    public StageDetails(
            @JsonProperty("id") String id,
            @JsonProperty("state") String state,
            @JsonProperty("tasks") List<TaskDetails> tasks,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("scheduledTime") BigDecimal scheduledTime,
            @JsonProperty("blockedTime") BigDecimal blockedTime,
            @JsonProperty("cpuTime") BigDecimal cpuTime,
            @JsonProperty("inputBuffer") long inputBuffer)
    {
        this.id = id;
        this.state = state;
        this.tasks = tasks;
        this.fullyBlocked = fullyBlocked;
        this.scheduledTime = scheduledTime;
        this.blockedTime = blockedTime;
        this.cpuTime = cpuTime;
        this.inputBuffer = inputBuffer;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public List<TaskDetails> getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public BigDecimal getScheduledTime()
    {
        return scheduledTime;
    }

    @JsonProperty
    public BigDecimal getBlockedTime()
    {
        return blockedTime;
    }

    @JsonProperty
    public BigDecimal getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public long getInputBuffer()
    {
        return inputBuffer;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, state, tasks);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StageDetails other = (StageDetails) o;
        return Objects.equals(this.id, other.id) &&
                Objects.equals(this.state, other.state) &&
                Objects.equals(this.tasks, other.tasks);
    }
}
