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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PartitionAndStatementId
{
    public static final JsonCodec<PartitionAndStatementId> CODEC = JsonCodec.jsonCodec(PartitionAndStatementId.class);

    private final String partitionName;
    private final int statementId;
    private final long rowCount;
    private final Optional<String> deleteDeltaDirectory;
    private final Optional<String> deltaDirectory;

    @JsonCreator
    public PartitionAndStatementId(
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("statementId") int statementId,
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("deleteDeltaDirectory") Optional<String> deleteDeltaDirectory,
            @JsonProperty("deltaDirectory") Optional<String> deltaDirectory)
    {
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.statementId = statementId;
        this.rowCount = rowCount;
        this.deleteDeltaDirectory = requireNonNull(deleteDeltaDirectory, "deleteDeltaDirectory is null");
        this.deltaDirectory = requireNonNull(deltaDirectory, "deltaDirectory is null");
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public int getStatementId()
    {
        return statementId;
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Optional<String> getDeleteDeltaDirectory()
    {
        return deleteDeltaDirectory;
    }

    @JsonProperty
    public Optional<String> getDeltaDirectory()
    {
        return deltaDirectory;
    }

    @JsonIgnore
    public List<String> getAllDirectories()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        deltaDirectory.ifPresent(builder::add);
        deleteDeltaDirectory.ifPresent(builder::add);
        return builder.build();
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
        PartitionAndStatementId that = (PartitionAndStatementId) o;
        return statementId == that.statementId &&
                rowCount == that.rowCount &&
                partitionName.equals(that.partitionName) &&
                deleteDeltaDirectory.equals(that.deleteDeltaDirectory) &&
                deltaDirectory.equals(that.deltaDirectory);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionName, statementId, rowCount, deleteDeltaDirectory, deltaDirectory);
    }
}
