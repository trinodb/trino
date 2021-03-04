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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.spi.data.FieldSpec.FieldType.DATE_TIME;

public class PinotColumn
{
    private final String name;
    private final Type type;
    private final FieldType fieldType;
    private final Optional<String> format;
    Optional<String> granularity;
    Optional<String> transform;

    public static PinotColumn fromFieldSpec(FieldSpec fieldSpec)
    {
        if (fieldSpec.getFieldType() == DATE_TIME) {
            checkState(fieldSpec instanceof DateTimeFieldSpec, "Unexpected type for dateTime column");
            DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
            return new PinotColumn(dateTimeFieldSpec.getName(),
                    getTrinoTypeFromPinotType(dateTimeFieldSpec),
                    dateTimeFieldSpec.getFieldType(),
                    Optional.of(dateTimeFieldSpec.getFormat()),
                    Optional.of(dateTimeFieldSpec.getGranularity()),
                    Optional.ofNullable(dateTimeFieldSpec.getTransformFunction()));
        }
        return new PinotColumn(fieldSpec.getName(),
                getTrinoTypeFromPinotType(fieldSpec),
                fieldSpec.getFieldType(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @JsonCreator
    public PinotColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("fieldType") FieldType fieldType,
            @JsonProperty("format") Optional<String> format,
            @JsonProperty("granularity") Optional<String> granularity,
            @JsonProperty("transform") Optional<String> transform)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.fieldType = requireNonNull(fieldType, "fieldType is null");
        this.format = requireNonNull(format, "format is null");
        this.granularity = requireNonNull(granularity, "granularity is null");
        this.transform = requireNonNull(transform, "transform is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public FieldType getFieldType()
    {
        return fieldType;
    }

    @JsonProperty
    public Optional<String> getFormat()
    {
        return format;
    }

    @JsonProperty
    public Optional<String> getGranularity()
    {
        return granularity;
    }

    @JsonProperty
    public Optional<String> getTransform()
    {
        return transform;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
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

        PinotColumn other = (PinotColumn) obj;
        return Objects.equals(this.name, other.name) && Objects.equals(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }

    public static List<PinotColumn> getPinotColumnsForPinotSchema(Schema pinotTableSchema)
    {
        return pinotTableSchema.getColumnNames().stream()
                .filter(columnName -> !columnName.startsWith("$")) // Hidden columns starts with "$", ignore them as we can't use them in PQL
                .map(columnName -> PinotColumn.fromFieldSpec(pinotTableSchema.getFieldSpecFor(columnName)))
                .collect(toImmutableList());
    }

    public static Type getTrinoTypeFromPinotType(FieldSpec field)
    {
        Type type = getTrinoTypeFromPinotType(field.getDataType());
        if (field.isSingleValueField()) {
            return type;
        }
        else {
            return new ArrayType(type);
        }
    }

    public static Type getTrinoTypeFromPinotType(DataType dataType)
    {
        switch (dataType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
                return VarbinaryType.VARBINARY;
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Not support type conversion for pinot data type: " + dataType);
    }
}
