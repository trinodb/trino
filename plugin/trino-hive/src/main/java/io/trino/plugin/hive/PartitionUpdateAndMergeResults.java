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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PartitionUpdateAndMergeResults
{
    public static final JsonCodec<PartitionUpdateAndMergeResults> CODEC = JsonCodec.jsonCodec(PartitionUpdateAndMergeResults.class);

    private final PartitionUpdate partitionUpdate;
    private final long insertRowCount;
    private final Optional<String> deltaDirectory;
    private final long deleteRowCount;
    private final Optional<String> deleteDeltaDirectory;

    @JsonCreator
    public PartitionUpdateAndMergeResults(
            @JsonProperty("partitionUpdate") PartitionUpdate partitionUpdate,
            @JsonProperty("insertRowCount") long insertRowCount,
            @JsonProperty("deleteDirectory") Optional<String> deltaDirectory,
            @JsonProperty("deleteRowCount") long deleteRowCount,
            @JsonProperty("deleteDeltaDirectory") Optional<String> deleteDeltaDirectory)
    {
        this.partitionUpdate = requireNonNull(partitionUpdate, "partitionUpdate is null");
        this.insertRowCount = insertRowCount;
        this.deltaDirectory = requireNonNull(deltaDirectory, "deltaDirectory is null");
        this.deleteRowCount = deleteRowCount;
        this.deleteDeltaDirectory = requireNonNull(deleteDeltaDirectory, "deleteDeltaDirectory is null");
    }

    @JsonProperty
    public PartitionUpdate getPartitionUpdate()
    {
        return partitionUpdate;
    }

    @JsonProperty
    public long getInsertRowCount()
    {
        return insertRowCount;
    }

    @JsonProperty
    public Optional<String> getDeltaDirectory()
    {
        return deltaDirectory;
    }

    @JsonProperty
    public long getDeleteRowCount()
    {
        return deleteRowCount;
    }

    @JsonProperty
    public Optional<String> getDeleteDeltaDirectory()
    {
        return deleteDeltaDirectory;
    }
}
