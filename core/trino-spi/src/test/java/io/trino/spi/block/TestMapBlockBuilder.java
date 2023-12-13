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
package io.trino.spi.block;

import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestMapBlockBuilder
        extends AbstractTestBlockBuilder<Map<String, Integer>>
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final MapType MAP_TYPE = new MapType(VARCHAR, INTEGER, TYPE_OPERATORS);

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new MapBlockBuilder(MAP_TYPE, null, 1);
    }

    @Override
    protected List<Map<String, Integer>> getTestValues()
    {
        return List.of(
                Map.of("a", 0, "apple", 1, "ape", 2),
                Map.of("b", 3, "banana", 4, "bear", 5, "break", 6),
                Map.of("c", 7, "cherry", 8),
                Map.of("d", 9, "date", 10, "dinosaur", 11, "dinner", 12, "dirt", 13),
                Map.of("e", 14, "eggplant", 15, "empty", 16, "", 17));
    }

    @Override
    protected Map<String, Integer> getUnusedTestValue()
    {
        return Map.of("unused", -1, "ignore me", -2);
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Map<String, Integer>> maps)
    {
        MapBlockBuilder blockBuilder = new MapBlockBuilder(MAP_TYPE, null, 1);
        for (Map<String, Integer> map : maps) {
            if (map == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    for (Map.Entry<String, Integer> entry : map.entrySet()) {
                        VARCHAR.writeString(keyBuilder, entry.getKey());
                        INTEGER.writeLong(valueBuilder, entry.getValue());
                    }
                });
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Map<String, Integer>> blockToValues(ValueBlock valueBlock)
    {
        MapBlock block = (MapBlock) valueBlock;
        List<Map<String, Integer>> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                SqlMap sqlMap = block.getMap(i);
                int rawOffset = sqlMap.getRawOffset();
                Block rawKeyBlock = sqlMap.getRawKeyBlock();
                Block rawValueBlock = sqlMap.getRawValueBlock();
                Map<String, Integer> actualMap = new HashMap<>();
                for (int entryIndex = 0; entryIndex < sqlMap.getSize(); entryIndex++) {
                    actualMap.put(
                            VARCHAR.getSlice(rawKeyBlock, rawOffset + entryIndex).toStringUtf8(),
                            INTEGER.getInt(rawValueBlock, rawOffset + entryIndex));
                }
                actualValues.add(actualMap);
            }
        }
        return actualValues;
    }
}
