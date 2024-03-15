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
package io.trino.plugin.deltalake.kernel;

import com.google.common.collect.ImmutableList;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;

/**
 * Utilities to convert between Trino and Delta Lake schemas/data types
 */
public class KernelSchemaUtils
{
    private KernelSchemaUtils() {}

    public static Type toTrinoType(SchemaTableName tableName, TypeManager typeManager, DataType deltaType)
    {
        checkArgument(deltaType != null);

        if (deltaType instanceof StructType) {
            StructType deltaStructType = (StructType) deltaType;
            ImmutableList.Builder<RowType.Field> fieldBuilder = ImmutableList.builder();
            deltaStructType.fields().stream()
                    .forEach(field -> {
                        String rowFieldName = field.getName().toLowerCase(Locale.US);
                        Type rowFieldType = toTrinoType(tableName, typeManager, field.getDataType());
                        fieldBuilder.add(new RowType.Field(Optional.of(rowFieldName), rowFieldType));
                    });
            return RowType.from(fieldBuilder.build());
        }
        else if (deltaType instanceof ArrayType) {
            ArrayType deltaArrayType = (ArrayType) deltaType;
            Type elementType = toTrinoType(tableName, typeManager, deltaArrayType.getElementType());
            return new io.trino.spi.type.ArrayType(elementType);
        }
        else if (deltaType instanceof MapType) {
            MapType deltaMapType = (MapType) deltaType;
            Type keyType = toTrinoType(tableName, typeManager, deltaMapType.getKeyType());
            Type valueType = toTrinoType(tableName, typeManager, deltaMapType.getValueType());
            return new io.trino.spi.type.MapType(keyType, valueType, typeManager.getTypeOperators());
        }

        return convertDeltaPrimitiveType(tableName, deltaType);
    }

    private static Type convertDeltaPrimitiveType(SchemaTableName tableName, DataType deltaType)
    {
        if (deltaType instanceof BinaryType) {
            return VARBINARY;
        }
        if (deltaType instanceof BooleanType) {
            return BOOLEAN;
        }
        else if (deltaType instanceof ByteType) {
            return TINYINT;
        }
        else if (deltaType instanceof DateType) {
            return DATE;
        }
        else if (deltaType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) deltaType;
            return createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }
        else if (deltaType instanceof DoubleType) {
            return DOUBLE;
        }
        else if (deltaType instanceof FloatType) {
            return REAL;
        }
        else if (deltaType instanceof IntegerType) {
            return INTEGER;
        }
        else if (deltaType instanceof LongType) {
            return BIGINT;
        }
        else if (deltaType instanceof ShortType) {
            return SMALLINT;
        }
        else if (deltaType instanceof StringType) {
            return createUnboundedVarcharType();
        }
        else if (deltaType instanceof TimestampType) {
            return TIMESTAMP_MICROS;
        }

        throw new TrinoException(
                DELTA_LAKE_INVALID_SCHEMA,
                format("Delta table %s contains unsupported data type: %s", tableName, deltaType));
    }
}
