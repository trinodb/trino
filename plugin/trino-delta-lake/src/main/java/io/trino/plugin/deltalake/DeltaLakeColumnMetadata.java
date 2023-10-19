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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DeltaLakeColumnMetadata
{
    private final ColumnMetadata columnMetadata;
    private final String name;
    private final OptionalInt fieldId;
    private final String physicalName;
    private final Type physicalColumnType;

    public DeltaLakeColumnMetadata(ColumnMetadata columnMetadata, String name, OptionalInt fieldId, String physicalName, Type physicalColumnType)
    {
        this.columnMetadata = requireNonNull(columnMetadata, "columnMetadata is null");
        this.name = requireNonNull(name, "name is null");
        this.fieldId = requireNonNull(fieldId, "fieldId is null");
        this.physicalName = requireNonNull(physicalName, "physicalName is null");
        this.physicalColumnType = requireNonNull(physicalColumnType, "physicalColumnType is null");
    }

    public ColumnMetadata getColumnMetadata()
    {
        return columnMetadata;
    }

    public OptionalInt getFieldId()
    {
        return fieldId;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return columnMetadata.getType();
    }

    public String getPhysicalName()
    {
        return physicalName;
    }

    public Type getPhysicalColumnType()
    {
        return physicalColumnType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnMetadata", columnMetadata)
                .add("name", name)
                .add("fieldId", fieldId)
                .add("physicalName", physicalName)
                .add("physicalColumnType", physicalColumnType)
                .toString();
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
        DeltaLakeColumnMetadata that = (DeltaLakeColumnMetadata) o;
        return Objects.equals(columnMetadata, that.columnMetadata) &&
                Objects.equals(name, that.name) &&
                Objects.equals(fieldId, that.fieldId) &&
                Objects.equals(physicalName, that.physicalName) &&
                Objects.equals(physicalColumnType, that.physicalColumnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnMetadata, name, fieldId, physicalName, physicalColumnType);
    }
}
