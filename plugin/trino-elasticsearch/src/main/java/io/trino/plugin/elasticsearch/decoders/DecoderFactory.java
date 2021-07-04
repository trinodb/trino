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
package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public final class DecoderFactory
{
    private static final Map<Type, Decoder> decoders = new ConcurrentHashMap<>();

    private DecoderFactory() {}

    // Initialize all primitive decoders.
    static {
        decoders.put(VARCHAR, new VarcharDecoder());
        decoders.put(VARBINARY, new VarbinaryDecoder());
        decoders.put(TIMESTAMP_MILLIS, new TimestampDecoder());
        decoders.put(BOOLEAN, new BooleanDecoder());
        decoders.put(DOUBLE, new DoubleDecoder());
        decoders.put(REAL, new RealDecoder());
        decoders.put(TINYINT, new TinyintDecoder());
        decoders.put(SMALLINT, new SmallintDecoder());
        decoders.put(INTEGER, new IntegerDecoder());
        decoders.put(BIGINT, new BigintDecoder());
        decoders.put(TIMESTAMP_MILLIS, new TimestampDecoder());
    }

    public static Decoder decoderOfType(Type type)
    {
        Decoder decoder = decoders.get(type);
        if (decoder == null) {
            switch (type.getBaseName()) {
                case IPADDRESS:
                    return decoders.computeIfAbsent(type, ipAddressType -> new IpAddressDecoder(ipAddressType));
                case ARRAY:
                    return new ArrayDecoder(decoderOfType(((ArrayType) type).getElementType()));
                case ROW:
                    final Map<String, Decoder> elementsDecoder = ((RowType) type).getFields().stream()
                            .collect(toImmutableMap(field -> field.getName().get(), field -> decoderOfType(field.getType())));
                    return new RowDecoder(elementsDecoder);
                default:
                    throw new TrinoException(NOT_SUPPORTED, format("Type '%s' not supported", type.getBaseName()));
            }
        }
        return decoder;
    }
}
