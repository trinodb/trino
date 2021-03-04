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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.pinot.PinotTableProperties.DEFAULT_VALUE_PROPERTY;

public class PinotTypeUtils
{
    private PinotTypeUtils() {}

    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BooleanType.BOOLEAN,
            TinyintType.TINYINT,
            SmallintType.SMALLINT,
            IntegerType.INTEGER,
            BigintType.BIGINT,
            RealType.REAL,
            DoubleType.DOUBLE,
            VarbinaryType.VARBINARY);

    private static final Map<Type, DataType> PRESTO_TO_PINOT_TYPE_MAP =
            ImmutableMap.<Type, DataType>builder()
                    .put(BooleanType.BOOLEAN, DataType.BOOLEAN)
                    .put(TinyintType.TINYINT, DataType.INT)
                    .put(SmallintType.SMALLINT, DataType.INT)
                    .put(IntegerType.INTEGER, DataType.INT)
                    .put(BigintType.BIGINT, DataType.LONG)
                    .put(RealType.REAL, DataType.FLOAT)
                    .put(DoubleType.DOUBLE, DataType.DOUBLE)
                    .put(VarcharType.VARCHAR, DataType.STRING)
                    .build();

    public static boolean isSupportedPrimitive(Type type)
    {
        return (type instanceof VarcharType) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    public static boolean isSupportedType(Type type)
    {
        if (isSupportedPrimitive(type)) {
            return true;
        }

        if (type instanceof ArrayType) {
            checkArgument(type.getTypeParameters().size() == 1, "expecting exactly one type parameter for array");
            return isSupportedType(type.getTypeParameters().get(0));
        }

        return false;
    }

    public static String getStringProperty(ColumnMetadata columnMetadata, String propertyName)
    {
        return (String) columnMetadata.getProperties().get(propertyName);
    }

    public static DataType getPinotType(Type prestoType)
    {
        return PRESTO_TO_PINOT_TYPE_MAP.get(prestoType);
    }

    public static Object getDefaultNullValue(ColumnMetadata columnMetadata)
    {
        String defaultNullValue = getStringProperty(columnMetadata, DEFAULT_VALUE_PROPERTY);
        if (defaultNullValue == null) {
            return null;
        }
        DataType pinotDataType = PRESTO_TO_PINOT_TYPE_MAP.get(columnMetadata.getType());
        switch (pinotDataType) {
            case STRING:
                return defaultNullValue;
            case INT:
                return Integer.parseInt(defaultNullValue);
            case LONG:
                return Long.parseLong(defaultNullValue);
            case FLOAT:
                return Float.parseFloat(defaultNullValue);
            case DOUBLE:
                return Double.parseDouble(defaultNullValue);
            default:
                throw new IllegalStateException();
        }
    }
}
