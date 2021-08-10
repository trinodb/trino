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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PinotColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type dataType;
    private final boolean returnNullOnEmptyGroup;

    public PinotColumnHandle(String columnName, Type dataType)
    {
        this(columnName, dataType, true);
    }

    @JsonCreator
    public PinotColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("dataType") Type dataType,
            @JsonProperty("returnNullOnEmptyGroup") boolean returnNullOnEmptyGroup)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.returnNullOnEmptyGroup = returnNullOnEmptyGroup;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getDataType()
    {
        return dataType;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(getColumnName(), getDataType());
    }

    // Some aggregations should return null on empty group, ex. min/max
    // If false then return the value from Pinot, ex. count(*)
    @JsonProperty
    public boolean isReturnNullOnEmptyGroup()
    {
        return returnNullOnEmptyGroup;
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

        PinotColumnHandle that = (PinotColumnHandle) o;
        return Objects.equals(getColumnName(), that.getColumnName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("dataType", dataType)
                .add("returnNullOnEmptyGroup", returnNullOnEmptyGroup)
                .toString();
    }
}
