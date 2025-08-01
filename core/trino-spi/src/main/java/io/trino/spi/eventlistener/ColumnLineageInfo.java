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

import java.util.Objects;
import java.util.Set;

/**
 * This class is JSON serializable for storing column lineage information for select queries.
 */
public class ColumnLineageInfo
{
    private final String name;
    private final int index;
    private final Set<ColumnDetail> sourceColumns;

    @JsonCreator
    public ColumnLineageInfo(@JsonProperty String outputExpression, @JsonProperty int index, @JsonProperty Set<ColumnDetail> sourceColumns)
    {
        this.name = outputExpression;
        this.index = index;
        this.sourceColumns = sourceColumns;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    /**
     * Returns the index of the column in the select list.
     * This is useful for identifying the position of the column in the result set.
     */
    @JsonProperty
    public int getIndex()
    {
        return index;
    }

    @JsonProperty
    public Set<ColumnDetail> getSourceColumns()
    {
        return sourceColumns;
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
        ColumnLineageInfo that = (ColumnLineageInfo) o;
        return Objects.equals(name, that.name)
                && Objects.equals(sourceColumns, that.sourceColumns)
                && index == that.index;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, sourceColumns, index);
    }
}
