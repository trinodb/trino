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
package io.trino.hive.formats.rcfile;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.stream.Collectors.toList;

public interface RcFileEncoding
{
    ColumnEncoding booleanEncoding(Type type);

    ColumnEncoding byteEncoding(Type type);

    ColumnEncoding shortEncoding(Type type);

    ColumnEncoding intEncoding(Type type);

    ColumnEncoding longEncoding(Type type);

    ColumnEncoding decimalEncoding(Type type);

    ColumnEncoding floatEncoding(Type type);

    ColumnEncoding doubleEncoding(Type type);

    ColumnEncoding stringEncoding(Type type);

    ColumnEncoding binaryEncoding(Type type);

    ColumnEncoding dateEncoding(Type type);

    ColumnEncoding timestampEncoding(TimestampType type);

    ColumnEncoding listEncoding(Type type, ColumnEncoding elementEncoding);

    ColumnEncoding mapEncoding(Type type, ColumnEncoding keyEncoding, ColumnEncoding valueEncoding);

    ColumnEncoding structEncoding(Type type, List<ColumnEncoding> fieldEncodings);

    default ColumnEncoding getEncoding(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return booleanEncoding(type);
        }
        if (TINYINT.equals(type)) {
            return byteEncoding(type);
        }
        if (SMALLINT.equals(type)) {
            return shortEncoding(type);
        }
        if (INTEGER.equals(type)) {
            return intEncoding(type);
        }
        if (BIGINT.equals(type)) {
            return longEncoding(type);
        }
        if (type instanceof DecimalType) {
            return decimalEncoding(type);
        }
        if (REAL.equals(type)) {
            return floatEncoding(type);
        }
        if (DOUBLE.equals(type)) {
            return doubleEncoding(type);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return stringEncoding(type);
        }
        if (VARBINARY.equals(type)) {
            return binaryEncoding(type);
        }
        if (DATE.equals(type)) {
            return dateEncoding(type);
        }
        if (type instanceof TimestampType) {
            return timestampEncoding((TimestampType) type);
        }
        if (type instanceof ArrayType) {
            ColumnEncoding elementType = getEncoding(type.getTypeParameters().get(0));
            return listEncoding(type, elementType);
        }
        if (type instanceof MapType) {
            ColumnEncoding keyType = getEncoding(type.getTypeParameters().get(0));
            ColumnEncoding valueType = getEncoding(type.getTypeParameters().get(1));
            return mapEncoding(type, keyType, valueType);
        }
        if (type instanceof RowType) {
            return structEncoding(
                    type,
                    type.getTypeParameters().stream()
                            .map(this::getEncoding)
                            .collect(toList()));
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
