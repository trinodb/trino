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
package io.trino.plugin.starrocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;

import java.util.Objects;

public class StarrocksColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final int ordinalPosition;
    private final String type;
    private final String columnType;
    private final boolean nullable;
    private final String extra;
    private final String comment;
    private final int columnSize;
    private final int decimalDigits;

    @JsonCreator
    public StarrocksColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("type") String type,
            @JsonProperty("column_type") String columnType,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("extra") String extra,
            @JsonProperty("comment") String comment,
            @JsonProperty("columnSize") int columnSize,
            @JsonProperty("decimalDigits") int decimalDigits)
    {
        this.columnName = columnName;
        this.ordinalPosition = ordinalPosition;
        this.type = type;
        this.columnType = columnType;
        this.nullable = nullable;
        this.extra = extra;
        this.comment = comment;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
    }

    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public String getExtra()
    {
        return extra;
    }

    @JsonProperty
    public int getColumnSize()
    {
        return columnSize;
    }

    @JsonProperty
    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, ordinalPosition, type, columnType, nullable, extra, comment, columnSize, decimalDigits);
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
        StarrocksColumnHandle other = (StarrocksColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.nullable, other.nullable) &&
                Objects.equals(this.extra, other.extra) &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.columnSize, other.columnSize) &&
                Objects.equals(this.decimalDigits, other.decimalDigits);
    }

    @Override
    public String toString()
    {
        return this.columnName + ":" +
                this.ordinalPosition + ":" +
                this.type + ":" +
                this.columnType + ":" +
                this.nullable + ":" +
                this.extra + ":" +
                this.comment + ":" +
                this.columnSize + ":" +
                this.decimalDigits;
    }
}
