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
package io.trino.spi.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.expression.FunctionName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ColumnStatisticMetadata
{
    private final String columnName;
    private final FunctionName aggregation;

    @JsonCreator
    public ColumnStatisticMetadata(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("aggregation") FunctionName aggregation)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.aggregation = requireNonNull(aggregation, "aggregation is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public FunctionName getAggregation()
    {
        return aggregation;
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
        ColumnStatisticMetadata that = (ColumnStatisticMetadata) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(aggregation, that.aggregation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, aggregation);
    }

    @Override
    public String toString()
    {
        return "ColumnStatisticMetadata{" +
                "columnName='" + columnName + '\'' +
                ", aggregation=" + aggregation +
                '}';
    }
}
