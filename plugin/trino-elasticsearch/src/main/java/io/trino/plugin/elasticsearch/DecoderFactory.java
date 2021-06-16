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
package io.trino.plugin.elasticsearch;

import io.trino.plugin.elasticsearch.decoders.ArrayDecoder;
import io.trino.plugin.elasticsearch.decoders.BigintDecoder;
import io.trino.plugin.elasticsearch.decoders.BooleanDecoder;
import io.trino.plugin.elasticsearch.decoders.Decoder;
import io.trino.plugin.elasticsearch.decoders.DoubleDecoder;
import io.trino.plugin.elasticsearch.decoders.IdColumnDecoder;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.IpAddressDecoder;
import io.trino.plugin.elasticsearch.decoders.RealDecoder;
import io.trino.plugin.elasticsearch.decoders.RowDecoder;
import io.trino.plugin.elasticsearch.decoders.ScoreColumnDecoder;
import io.trino.plugin.elasticsearch.decoders.SmallintDecoder;
import io.trino.plugin.elasticsearch.decoders.SourceColumnDecoder;
import io.trino.plugin.elasticsearch.decoders.TimestampDecoder;
import io.trino.plugin.elasticsearch.decoders.TinyintDecoder;
import io.trino.plugin.elasticsearch.decoders.VarbinaryDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.BuiltinColumns.ID;
import static io.trino.plugin.elasticsearch.BuiltinColumns.SCORE;
import static io.trino.plugin.elasticsearch.BuiltinColumns.SOURCE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DecoderFactory
{
    private DecoderFactory()
    {
    }

    public static List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> {
                    if (column.getName().equals(ID.getName())) {
                        return new IdColumnDecoder();
                    }

                    if (column.getName().equals(SCORE.getName())) {
                        return new ScoreColumnDecoder();
                    }

                    if (column.getName().equals(SOURCE.getName())) {
                        return new SourceColumnDecoder();
                    }

                    return DecoderFactory.createDecoder(column.getName(), column.getType());
                })
                .collect(toImmutableList());
    }

    public static Decoder createDecoder(String path, Type type)
    {
        if (type.equals(VARCHAR)) {
            return new VarcharDecoder(path);
        }
        if (type.equals(VARBINARY)) {
            return new VarbinaryDecoder(path);
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return new TimestampDecoder(path);
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
                    .map(field -> createDecoder(appendPath(path, field.getName().get()), field.getType()))
                    .collect(toImmutableList());

            List<String> fieldNames = rowType.getFields().stream()
                    .map(RowType.Field::getName)
                    .map(Optional::get)
                    .collect(toImmutableList());

            return new RowDecoder(path, fieldNames, decoders);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();

            return new ArrayDecoder(createDecoder(path, elementType));
        }

        throw new UnsupportedOperationException("Type not supported: " + type);
    }

    private static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }
}
