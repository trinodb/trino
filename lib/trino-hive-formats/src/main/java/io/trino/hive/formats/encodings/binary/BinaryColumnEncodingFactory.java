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
package io.trino.hive.formats.encodings.binary;

import io.trino.hive.formats.encodings.ColumnEncodingFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.joda.time.DateTimeZone;

import java.util.stream.Collectors;

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
import static java.util.Objects.requireNonNull;

public class BinaryColumnEncodingFactory
        implements ColumnEncodingFactory
{
    private final DateTimeZone timeZone;

    public BinaryColumnEncodingFactory(DateTimeZone timeZone)
    {
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
    }

    @Override
    public BinaryColumnEncoding getEncoding(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanEncoding(type);
        }
        if (TINYINT.equals(type)) {
            return new ByteEncoding(type);
        }
        if (SMALLINT.equals(type)) {
            return new ShortEncoding(type);
        }
        if (INTEGER.equals(type) || BIGINT.equals(type)) {
            return new LongEncoding(type);
        }
        if (type instanceof DecimalType) {
            return new DecimalEncoding(type);
        }
        if (REAL.equals(type)) {
            return new FloatEncoding(type);
        }
        if (DOUBLE.equals(type)) {
            return new DoubleEncoding(type);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return new StringEncoding(type);
        }
        if (VARBINARY.equals(type)) {
            return new BinaryEncoding(type);
        }
        if (DATE.equals(type)) {
            return new DateEncoding(type);
        }
        if (type instanceof TimestampType) {
            return new TimestampEncoding((TimestampType) type, timeZone);
        }
        if (type instanceof ArrayType) {
            return new ListEncoding(type, getEncoding(type.getTypeParameters().get(0)));
        }
        if (type instanceof MapType) {
            return new MapEncoding(
                    type,
                    getEncoding(type.getTypeParameters().get(0)),
                    getEncoding(type.getTypeParameters().get(1)));
        }
        if (type instanceof RowType) {
            return new StructEncoding(
                    type,
                    type.getTypeParameters().stream()
                            .map(this::getEncoding)
                            .collect(Collectors.toList()));
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
