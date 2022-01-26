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
package io.trino.plugin.deltalake.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DeltaLakeStatistics
{
    public static final long CURRENT_MODEL_VERSION = 4;

    private final long modelVersion;
    private final Instant alreadyAnalyzedModifiedTimeMax;
    private final Map<String, DeltaLakeColumnStatistics> columnStatistics;
    private final Optional<Set<String>> analyzedColumns;

    public DeltaLakeStatistics(
            Instant alreadyAnalyzedModifiedTimeMax,
            Map<String, DeltaLakeColumnStatistics> columnStatistics,
            Optional<Set<String>> analyzedColumns)
    {
        this(CURRENT_MODEL_VERSION, alreadyAnalyzedModifiedTimeMax, columnStatistics, analyzedColumns);
    }

    @JsonCreator
    public DeltaLakeStatistics(
            @JsonProperty("modelVersion") long modelVersion,
            @JsonProperty("alreadyAnalyzedModifiedTimeMax") Instant alreadyAnalyzedModifiedTimeMax,
            @JsonProperty("columnStatistics") Map<String, DeltaLakeColumnStatistics> columnStatistics,
            @JsonProperty("analyzedColumns") Optional<Set<String>> analyzedColumns)
    {
        this.modelVersion = modelVersion;
        this.alreadyAnalyzedModifiedTimeMax = alreadyAnalyzedModifiedTimeMax;
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
        requireNonNull(analyzedColumns, "analyzedColumns is null");
        this.analyzedColumns = analyzedColumns.map(ImmutableSet::copyOf);
    }

    @JsonProperty
    public long getModelVersion()
    {
        return modelVersion;
    }

    @JsonProperty
    public Instant getAlreadyAnalyzedModifiedTimeMax()
    {
        return alreadyAnalyzedModifiedTimeMax;
    }

    @JsonProperty
    public Map<String, DeltaLakeColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    @JsonProperty
    public Optional<Set<String>> getAnalyzedColumns()
    {
        return analyzedColumns;
    }
}
