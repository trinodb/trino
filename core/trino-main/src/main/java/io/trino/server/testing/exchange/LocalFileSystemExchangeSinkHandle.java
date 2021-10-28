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
package io.trino.server.testing.exchange;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeSinkHandle;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSinkHandle
        implements ExchangeSinkHandle
{
    private final QueryId queryId;
    private final int stageId;
    private final int taskPartitionId;

    @JsonCreator
    public LocalFileSystemExchangeSinkHandle(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("stageId") int stageId,
            @JsonProperty("taskPartitionId") int taskPartitionId)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.stageId = stageId;
        this.taskPartitionId = taskPartitionId;
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getTaskPartitionId()
    {
        return taskPartitionId;
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
        LocalFileSystemExchangeSinkHandle sinkHandle = (LocalFileSystemExchangeSinkHandle) o;
        return stageId == sinkHandle.stageId && taskPartitionId == sinkHandle.taskPartitionId && Objects.equals(queryId, sinkHandle.queryId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryId, stageId, taskPartitionId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("stageId", stageId)
                .add("taskPartitionId", taskPartitionId)
                .toString();
    }
}
