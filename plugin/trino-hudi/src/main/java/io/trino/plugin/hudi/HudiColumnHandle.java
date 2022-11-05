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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HudiColumnHandle
        implements ConnectorTableHandle
{
    private final String columnName;
    private final HiveColumnHandle.ColumnType columnType;
    private final HiveType hiveType;
    private final Type baseType;
    private final Schema.Type avroType;
    private final boolean nullable;

    @JsonCreator
    public HudiColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") HiveColumnHandle.ColumnType columnType,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("avroType") Schema.Type avroType,
            @JsonProperty("nullable") Boolean nullable)
    {
        this.columnName = requireNonNull(columnName, "baseColumnName is null");
        this.hiveType = requireNonNull(hiveType, "baseHiveType is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.avroType = requireNonNull(avroType, "avroType is null");
        this.nullable = nullable;
    }

    public static HudiColumnHandle createHudiColumn(
            String columnName,
            HiveColumnHandle.ColumnType columnType,
            HiveType hiveType,
            Type type,
            Schema.Type avroType)
    {
        return new HudiColumnHandle(columnName, columnType, hiveType, type, avroType, true);
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public HiveColumnHandle.ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    @JsonProperty
    public Schema.Type getAvroType()
    {
        return avroType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, hiveType, baseType, columnType, avroType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HudiColumnHandle other = (HudiColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.hiveType, other.hiveType) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.avroType, other.avroType);
    }

    @Override
    public String toString()
    {
        return columnName + ":" + getHiveType() + ":" + columnType;
    }

    public Column toColumn()
    {
        return new Column(this.columnName, this.hiveType, Optional.empty());
    }
}
