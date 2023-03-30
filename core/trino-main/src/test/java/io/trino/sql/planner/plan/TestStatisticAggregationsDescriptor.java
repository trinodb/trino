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
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodec;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.testng.annotations.Test;

import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestStatisticAggregationsDescriptor
{
    private static final ImmutableList<String> COLUMNS = ImmutableList.of("", "col1", "$:###:;", "abc+dddd___");

    @Test
    public void testSerializationRoundTrip()
    {
        JsonCodec<StatisticAggregationsDescriptor<Symbol>> codec = JsonCodec.jsonCodec(new TypeToken<>() {});
        assertSerializationRoundTrip(codec, StatisticAggregationsDescriptor.<Symbol>builder().build());
        assertSerializationRoundTrip(codec, createTestDescriptor());
    }

    private static void assertSerializationRoundTrip(JsonCodec<StatisticAggregationsDescriptor<Symbol>> codec, StatisticAggregationsDescriptor<Symbol> descriptor)
    {
        assertEquals(codec.fromJson(codec.toJson(descriptor)), descriptor);
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
