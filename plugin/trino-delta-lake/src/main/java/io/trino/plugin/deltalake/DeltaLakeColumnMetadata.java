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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DeltaLakeColumnMetadata
{
    private final ColumnMetadata columnMetadata;
    private final String physicalName;
    private final Type physicalColumnType;

    public DeltaLakeColumnMetadata(ColumnMetadata columnMetadata, String physicalName, Type physicalColumnType)
    {
        this.columnMetadata = requireNonNull(columnMetadata, "columnMetadata is null");
        this.physicalName = physicalName.toLowerCase(ENGLISH);
        this.physicalColumnType = requireNonNull(physicalColumnType, "physicalColumnType is null");
    }

    public ColumnMetadata getColumnMetadata()
    {
        return columnMetadata;
    }

    public String getName()
    {
        return columnMetadata.getName();
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
                Objects.equals(physicalName, that.physicalName) &&
                Objects.equals(physicalColumnType, that.physicalColumnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnMetadata, physicalName, physicalColumnType);
    }
}
