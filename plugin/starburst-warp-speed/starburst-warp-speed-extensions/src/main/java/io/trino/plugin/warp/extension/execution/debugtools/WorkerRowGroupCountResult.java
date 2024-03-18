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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

public record WorkerRowGroupCountResult(
        long count,
        Set<String> warmupColumnNames,
        Map<String, Integer> warmupColumnCount,
        Set<String> rowGroupFilePathSet)
{
    @JsonCreator
    public WorkerRowGroupCountResult(
            @JsonProperty("count") long count,
            @JsonProperty("warmupColumnNames") Set<String> warmupColumnNames,
            @JsonProperty("warmupColumnCount") Map<String, Integer> warmupColumnCount,
            @JsonProperty("rowGroupFilePathSet") Set<String> rowGroupFilePathSet)
    {
        this.count = count;
        this.warmupColumnNames = warmupColumnNames;
        this.warmupColumnCount = warmupColumnCount;
        this.rowGroupFilePathSet = rowGroupFilePathSet;
    }
}
