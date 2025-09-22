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

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * This record is JSON serializable for storing column lineage information for select queries.
 */
public record ColumnLineageInfo(
        @JsonProperty String name,
        @JsonProperty Set<ColumnDetail> sourceColumns)
{
    @JsonCreator
    public ColumnLineageInfo(String name, Set<ColumnDetail> sourceColumns)
    {
        requireNonNull(name, "name is null");
        requireNonNull(sourceColumns, "sourceColumns is null");
        this.name = name;
        this.sourceColumns = Set.copyOf(sourceColumns);
    }
}
