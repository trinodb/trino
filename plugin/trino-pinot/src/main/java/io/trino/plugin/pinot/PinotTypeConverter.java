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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
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
import org.apache.pinot.spi.data.FieldSpec;

import javax.inject.Inject;

import java.util.Optional;
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
        return toTrinoType(field.getDataType(), field.isSingleValueField());
    }

    public Type toTrinoType(TransformResultMetadata transformResultMetadata)
    {
        return toTrinoType(transformResultMetadata.getDataType(), transformResultMetadata.isSingleValue());
    }

    private Type toTrinoType(FieldSpec.DataType dataType, boolean isSingleValue)
    {
        Type type = toTrinoType(dataType);
        if (isSingleValue) {
            return type;
        }
        return new ArrayType(type);
    }

    private Type toTrinoType(FieldSpec.DataType dataType)
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
            case JSON:
                return jsonTypeSupplier.get();
            case BYTES:
                return VarbinaryType.VARBINARY;
            case TIMESTAMP:
                return TimestampType.TIMESTAMP_MILLIS;
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported type conversion for pinot data type: " + dataType);
    }

    public Type toTrinoType(DataSchema.ColumnDataType columnDataType)
    {
        switch (columnDataType) {
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case STRING:
                return VarcharType.VARCHAR;
            case JSON:
                return jsonTypeSupplier.get();
            case BYTES:
                return VarbinaryType.VARBINARY;
            case INT_ARRAY:
                return new ArrayType(IntegerType.INTEGER);
            case LONG_ARRAY:
                return new ArrayType(BigintType.BIGINT);
            case DOUBLE_ARRAY:
                return new ArrayType(DoubleType.DOUBLE);
            case STRING_ARRAY:
                return new ArrayType(VarcharType.VARCHAR);
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported column data type: " + columnDataType);
    }

    public boolean isJsonType(Type type)
    {
        requireNonNull(type, "type is null");
        return type.equals(jsonTypeSupplier.get());
    }
}
