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

import io.trino.plugin.elasticsearch.decoders.IdColumnDecoder;
import io.trino.plugin.elasticsearch.decoders.ScoreColumnDecoder;
import io.trino.plugin.elasticsearch.decoders.SourceColumnDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;

enum BuiltinColumns
{
    ID("_id", VARCHAR, new IdColumnDecoder.Descriptor(), true),
    SOURCE("_source", VARCHAR, new SourceColumnDecoder.Descriptor(), false),
    SCORE("_score", REAL, new ScoreColumnDecoder.Descriptor(), false);

    private static final Map<String, BuiltinColumns> COLUMNS_BY_NAME = stream(values())
            .collect(toImmutableMap(BuiltinColumns::getName, identity()));

    private final String name;
    private final Type type;
    private final DecoderDescriptor decoderDescriptor;
    private final boolean supportsPredicates;

    BuiltinColumns(String name, Type type, DecoderDescriptor decoderDescriptor, boolean supportsPredicates)
    {
        this.name = name;
        this.type = type;
        this.decoderDescriptor = decoderDescriptor;
        this.supportsPredicates = supportsPredicates;
    }

    public static Optional<BuiltinColumns> of(String name)
    {
        return Optional.ofNullable(COLUMNS_BY_NAME.get(name));
    }

    public static boolean isBuiltinColumn(String name)
    {
        return COLUMNS_BY_NAME.containsKey(name);
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public ColumnMetadata getMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setHidden(true)
                .build();
    }

    public ColumnHandle getColumnHandle()
    {
        return new ElasticsearchColumnHandle(
                name,
                type,
                decoderDescriptor,
                supportsPredicates);
    }
}
