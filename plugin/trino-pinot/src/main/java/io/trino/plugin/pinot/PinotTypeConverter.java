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

import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static java.util.Objects.requireNonNull;

public class PinotTypeConverter
{
    // Supplier is used for compatibility with unit tests using TestingTypeManager.
    // TestingTypeManager does not support json type.
    private final Supplier<Type> jsonTypeSupplier;

    @Inject
    public PinotTypeConverter(TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.jsonTypeSupplier = Suppliers.memoize(() -> typeManager.getType(new TypeSignature(StandardTypes.JSON)));
    }

    public Type toTrinoType(FieldSpec field)
    {
        return toTrinoType(field.getDataType(), field.isSingleValueField(), getFormatSpec(field));
    }

    private static Optional<DateTimeFormatSpec> getFormatSpec(FieldSpec field)
    {
        if (field instanceof DateTimeFieldSpec spec) {
            return Optional.of(spec.getFormatSpec());
        }
        return Optional.empty();
    }

    private static boolean isDateType(DateTimeFormatSpec formatSpec)
    {
        return formatSpec.getColumnUnit() == TimeUnit.DAYS &&
                formatSpec.getTimeFormat() == DateTimeFieldSpec.TimeFormat.EPOCH &&
                formatSpec.getColumnSize() == 1;
    }

    public Type toTrinoType(TransformResultMetadata transformResultMetadata)
    {
        return toTrinoType(transformResultMetadata.getDataType(), transformResultMetadata.isSingleValue(), Optional.empty());
    }

    private Type toTrinoType(FieldSpec.DataType dataType, boolean isSingleValue, Optional<DateTimeFormatSpec> formatSpec)
    {
        Type type = toTrinoType(dataType, formatSpec);
        if (isSingleValue) {
            return type;
        }
        return new ArrayType(type);
    }

    private Type toTrinoType(FieldSpec.DataType dataType, Optional<DateTimeFormatSpec> formatSpec)
    {
        return switch (dataType) {
            case BOOLEAN -> BooleanType.BOOLEAN;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case INT -> {
                if (formatSpec.map(PinotTypeConverter::isDateType).orElse(false)) {
                    yield DateType.DATE;
                }
                yield IntegerType.INTEGER;
            }
            case LONG -> {
                if (formatSpec.map(PinotTypeConverter::isDateType).orElse(false)) {
                    yield DateType.DATE;
                }
                yield BigintType.BIGINT;
            }
            case STRING -> VarcharType.VARCHAR;
            case JSON -> jsonTypeSupplier.get();
            case BYTES -> VarbinaryType.VARBINARY;
            case TIMESTAMP -> TimestampType.TIMESTAMP_MILLIS;
            default -> throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported type conversion for pinot data type: " + dataType);
        };
    }

    public Type toTrinoType(DataSchema.ColumnDataType columnDataType)
    {
        return switch (columnDataType) {
            case INT -> IntegerType.INTEGER;
            case LONG -> BigintType.BIGINT;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case STRING -> VarcharType.VARCHAR;
            case JSON -> jsonTypeSupplier.get();
            case BYTES -> VarbinaryType.VARBINARY;
            case INT_ARRAY -> new ArrayType(IntegerType.INTEGER);
            case LONG_ARRAY -> new ArrayType(BigintType.BIGINT);
            case DOUBLE_ARRAY -> new ArrayType(DoubleType.DOUBLE);
            case STRING_ARRAY -> new ArrayType(VarcharType.VARCHAR);
            default -> throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported column data type: " + columnDataType);
        };
    }

    public boolean isJsonType(Type type)
    {
        requireNonNull(type, "type is null");
        return type.equals(jsonTypeSupplier.get());
    }
}
