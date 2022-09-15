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
import java.util.Objects;

public class TaskDetails
{
    private final String id;
    private final String nodeHost;
    private final String state;
    private final int queuedSplits;
    private final int runningSplits;
    private final int blockedSplits;
    private final int completedSplits;
    private final int totalSplits;
    private final long rowsRead;
    private final long bytesRead;
    private final long bytesBuffered;
    private final BigDecimal elapsedTime;
    private final BigDecimal cpuTime;
    private final long physicalWrittenDataSize;
    private final long physicalInputDataSize;

    @JsonCreator
    public TaskDetails(
            @JsonProperty("id") String id,
            @JsonProperty("nodeHost") String nodeHost,
            @JsonProperty("state") String state,
            @JsonProperty("queuedSplits") int queuedSplits,
            @JsonProperty("runningSplits") int runningSplits,
            @JsonProperty("blockedSplits") int blockedSplits,
            @JsonProperty("completedSplits") int completedSplits,
            @JsonProperty("totalSplits") int totalSplits,
            @JsonProperty("rowsRead") long rowsRead,
            @JsonProperty("bytesRead") long bytesRead,
            @JsonProperty("bytesBuffered") long bytesBuffered,
            @JsonProperty("elapsedTime") BigDecimal elapsedTime,
            @JsonProperty("cpuTime") BigDecimal cpuTime,
            @JsonProperty("physicalWrittenDataSize") long physicalWrittenDataSize,
            @JsonProperty("physicalInputDataSize") long physicalInputDataSize)
    {
        this.id = id;
        this.nodeHost = nodeHost;
        this.state = state;
        this.queuedSplits = queuedSplits;
        this.runningSplits = runningSplits;
        this.blockedSplits = blockedSplits;
        this.completedSplits = completedSplits;
        this.totalSplits = totalSplits;
        this.rowsRead = rowsRead;
        this.bytesRead = bytesRead;
        this.bytesBuffered = bytesBuffered;
        this.elapsedTime = elapsedTime;
        this.cpuTime = cpuTime;
        this.physicalWrittenDataSize = physicalWrittenDataSize;
        this.physicalInputDataSize = physicalInputDataSize;
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getNodeHost()
    {
        return nodeHost;
    }

    @JsonProperty
    public String getState()
    {
        return state;
    }

    @JsonProperty
    public int getQueuedSplits()
    {
        return queuedSplits;
    }

    @JsonProperty
    public int getRunningSplits()
    {
        return runningSplits;
    }

    @JsonProperty
    public int getBlockedSplits()
    {
        return blockedSplits;
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @JsonProperty
    public int getTotalSplits()
    {
        return totalSplits;
    }

    @JsonProperty
    public long getRowsRead()
    {
        return rowsRead;
    }

    @JsonProperty
    public long getBytesRead()
    {
        return bytesRead;
    }

    @JsonProperty
    public long getBytesBuffered()
    {
        return bytesBuffered;
    }

    @JsonProperty
    public BigDecimal getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public BigDecimal getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public long getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public long getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, nodeHost, state);
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
        TaskDetails other = (TaskDetails) o;
        return Objects.equals(this.id, other.id) &&
                Objects.equals(this.nodeHost, other.nodeHost) &&
                Objects.equals(this.state, other.state);
    }
}
