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
package io.trino.plugin.hudi.compaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.plugin.hudi.files.HudiFileGroupId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hudi.files.FSUtils.getCommitTime;
import static java.util.Objects.requireNonNull;

public class CompactionOperation
{
    private String baseInstantTime;
    private Optional<String> dataFileCommitTime;
    private List<String> deltaFileNames;
    private Optional<String> dataFileName;
    private HudiFileGroupId id;
    private Map<String, Double> metrics;
    private Optional<String> bootstrapFilePath;

    public CompactionOperation(
            String baseInstantTime,
            Optional<String> dataFileCommitTime,
            List<String> deltaFileNames,
            Optional<String> dataFileName,
            HudiFileGroupId id,
            Map<String, Double> metrics,
            Optional<String> bootstrapFilePath)
    {
        this.baseInstantTime = requireNonNull(baseInstantTime, "baseInstantTime is null");
        this.dataFileCommitTime = requireNonNull(dataFileCommitTime, "dataFileCommitTime is null");
        this.deltaFileNames = requireNonNull(deltaFileNames, "deltaFileNames is null");
        this.dataFileName = requireNonNull(dataFileName, "dataFileName is null");
        this.id = requireNonNull(id, "id is null");
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.bootstrapFilePath = requireNonNull(bootstrapFilePath, "bootstrapFilePath is null");
    }

    public String getFileId()
    {
        return id.getFileId();
    }

    public String getPartitionPath()
    {
        return id.getPartitionPath();
    }

    public HudiFileGroupId getFileGroupId()
    {
        return id;
    }

    public static CompactionOperation convertFromAvroRecordInstance(HudiCompactionOperation operation)
    {
        Optional<String> dataFileName = Optional.ofNullable(operation.getDataFilePath());
        return new CompactionOperation(
                operation.getBaseInstantTime(),
                dataFileName.map(path -> getCommitTime(Location.of(path).fileName())),
                ImmutableList.copyOf(operation.getDeltaFilePaths()),
                dataFileName,
                new HudiFileGroupId(operation.getPartitionPath(), operation.getFileId()),
                operation.getMetrics() == null ? ImmutableMap.of() : ImmutableMap.copyOf(operation.getMetrics()),
                Optional.ofNullable(operation.getBootstrapFilePath()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("baseInstantTime", baseInstantTime)
                .add("dataFileCommitTime", dataFileCommitTime)
                .add("deltaFileNames", deltaFileNames)
                .add("dataFileName", dataFileName)
                .add("id", id)
                .add("metrics", metrics)
                .add("bootstrapFilePath", bootstrapFilePath)
                .toString();
    }
}
