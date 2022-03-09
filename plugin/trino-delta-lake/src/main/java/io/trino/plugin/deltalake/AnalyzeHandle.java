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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AnalyzeHandle
{
    private final long tableVersion;
    private final boolean initialAnalyze;
    private final Optional<Instant> filesModifiedAfter;
    private final Optional<Set<String>> columns;

    @JsonCreator
    public AnalyzeHandle(
            @JsonProperty("tableVersion") long tableVersion,
            @JsonProperty("initialAnalyze") boolean initialAnalyze,
            @JsonProperty("startTime") Optional<Instant> filesModifiedAfter,
            @JsonProperty("columns") Optional<Set<String>> columns)
    {
        this.tableVersion = tableVersion;
        this.initialAnalyze = initialAnalyze;
        this.filesModifiedAfter = requireNonNull(filesModifiedAfter, "filesModifiedAfter is null");
        requireNonNull(columns, "columns is null");
        this.columns = columns.map(ImmutableSet::copyOf);
    }

    @JsonProperty
    public long getTableVersion()
    {
        return tableVersion;
    }

    @JsonProperty
    public boolean isInitialAnalyze()
    {
        return initialAnalyze;
    }

    @JsonProperty
    public Optional<Instant> getFilesModifiedAfter()
    {
        return filesModifiedAfter;
    }

    @JsonProperty
    public Optional<Set<String>> getColumns()
    {
        return columns;
    }
}
