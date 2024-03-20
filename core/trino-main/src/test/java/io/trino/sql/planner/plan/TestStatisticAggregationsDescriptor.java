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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.junit.jupiter.api.Test;

import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatisticAggregationsDescriptor
{
    private static final ImmutableList<String> COLUMNS = ImmutableList.of("", "col1", "$:###:;", "abc+dddd___");

    @Test
    public void testSerializationRoundTrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeyDeserializers(ImmutableMap.of(
                Symbol.class, new SymbolKeyDeserializer(new TestingTypeManager()),
                TypeSignature.class, new TypeSignatureKeyDeserializer()));

        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(new TestingTypeManager()::getType)));

        JsonCodec<StatisticAggregationsDescriptor<Symbol>> codec = new JsonCodecFactory(provider).jsonCodec(new TypeToken<>() {});
        assertSerializationRoundTrip(codec, StatisticAggregationsDescriptor.<Symbol>builder().build());
        assertSerializationRoundTrip(codec, createTestDescriptor());
    }

    private static void assertSerializationRoundTrip(JsonCodec<StatisticAggregationsDescriptor<Symbol>> codec, StatisticAggregationsDescriptor<Symbol> descriptor)
    {
        assertThat(codec.fromJson(codec.toJson(descriptor))).isEqualTo(descriptor);
    }

    private static StatisticAggregationsDescriptor<Symbol> createTestDescriptor()
    {
        StatisticAggregationsDescriptor.Builder<Symbol> builder = StatisticAggregationsDescriptor.builder();
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        for (String column : COLUMNS) {
            for (ColumnStatisticType type : ColumnStatisticType.values()) {
                builder.addColumnStatistic(new ColumnStatisticMetadata(column, type), testSymbol(symbolAllocator));
            }
            builder.addColumnStatistic(new ColumnStatisticMetadata(column, "count non null", new FunctionName("count")), testSymbol(symbolAllocator));
            builder.addColumnStatistic(new ColumnStatisticMetadata(column, "count true", new FunctionName("count_if")), testSymbol(symbolAllocator));
            builder.addGrouping(column, testSymbol(symbolAllocator));
        }
        builder.addTableStatistic(ROW_COUNT, testSymbol(symbolAllocator));
        return builder.build();
    }

    private static Symbol testSymbol(SymbolAllocator allocator)
    {
        return allocator.newSymbol("test_symbol", BIGINT);
    }
}
