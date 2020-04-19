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
package io.prestosql.elasticsearch.decoders;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class CoderFactory
{
    private CoderFactory()
    {
        //
    }

    public static Decoder createDecoder(ConnectorSession session, String path, Type type)
    {
        if (type.equals(VARCHAR)) {
            return new VarcharDecoder(path);
        }
        if (type.equals(VARBINARY)) {
            return new VarbinaryDecoder(path);
        }
        if (type.equals(TIMESTAMP)) {
            return new TimestampDecoder(session, path);
        }
        if (type.equals(BOOLEAN)) {
            return new BooleanDecoder(path);
        }
        if (type.equals(DOUBLE)) {
            return new DoubleDecoder(path);
        }
        if (type.equals(REAL)) {
            return new RealDecoder(path);
        }
        if (type.equals(TINYINT)) {
            return new TinyintDecoder(path);
        }
        if (type.equals(SMALLINT)) {
            return new SmallintDecoder(path);
        }
        if (type.equals(INTEGER)) {
            return new IntegerDecoder(path);
        }
        if (type.equals(BIGINT)) {
            return new BigintDecoder(path);
        }
        if (type.getBaseName().equals(StandardTypes.IPADDRESS)) {
            return new IpAddressDecoder(path, type);
        }
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;

            List<Decoder> decoders = rowType.getFields().stream()
                    .map(field -> createDecoder(session, appendPath(path, field.getName().get()), field.getType()))
                    .collect(toImmutableList());

            List<String> fieldNames = rowType.getFields().stream()
                    .map(RowType.Field::getName)
                    .map(Optional::get)
                    .collect(toImmutableList());

            return new RowDecoder(path, fieldNames, decoders);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();

            return new ArrayDecoder(path, createDecoder(session, path, elementType));
        }

        throw new UnsupportedOperationException("Type not supported: " + type);
    }

    public static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }
}
