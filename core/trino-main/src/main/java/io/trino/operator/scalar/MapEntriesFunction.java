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
package io.trino.operator.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static com.google.common.base.Verify.verify;

@ScalarFunction("map_entries")
@Description("Construct an array of entries from a given map")
public class MapEntriesFunction
{
    private final BufferedArrayValueBuilder arrayValueBuilder;

    @TypeParameter("K")
    @TypeParameter("V")
    public MapEntriesFunction(@TypeParameter("array(row(K,V))") Type arrayType)
    {
        arrayValueBuilder = BufferedArrayValueBuilder.createBuffered((ArrayType) arrayType);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("array(row(K,V))")
    public Block mapFromEntries(
            @TypeParameter("row(K,V)") RowType rowType,
            @SqlType("map(K,V)") SqlMap sqlMap)
    {
        verify(rowType.getTypeParameters().size() == 2);

        int size = sqlMap.getSize();
        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        return arrayValueBuilder.build(size, valueBuilder -> {
            ValueBlock keyBlock = rawKeyBlock.getUnderlyingValueBlock();
            ValueBlock valueBlock = rawValueBlock.getUnderlyingValueBlock();
            for (int i = 0; i < size; i++) {
                int offset = rawOffset + i;
                ((RowBlockBuilder) valueBuilder).buildEntry(fieldBuilders -> {
                    fieldBuilders.get(0).append(keyBlock, rawKeyBlock.getUnderlyingValuePosition(offset));
                    fieldBuilders.get(1).append(valueBlock, rawValueBlock.getUnderlyingValuePosition(offset));
                });
            }
        });
    }
}
