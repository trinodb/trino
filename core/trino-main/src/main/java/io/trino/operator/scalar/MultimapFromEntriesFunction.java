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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;

@ScalarFunction("multimap_from_entries")
@Description("Construct a multimap from an array of entries")
public final class MultimapFromEntriesFunction
{
    private static final int INITIAL_ENTRY_COUNT = 128;

    private final BufferedMapValueBuilder mapValueBuilder;
    private IntList[] entryIndicesList;

    @TypeParameter("K")
    @TypeParameter("V")
    public MultimapFromEntriesFunction(@TypeParameter("map(K,array(V))") Type mapType)
    {
        mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        initializeEntryIndicesList(INITIAL_ENTRY_COUNT);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("map(K,array(V))")
    @SqlNullable
    public Block multimapFromEntries(
            @TypeParameter("map(K,array(V))") MapType mapType,
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockPositionIsDistinctFrom keysDistinctOperator,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL)) BlockPositionHashCode keyHashCode,
            @SqlType("array(row(K,V))") Block mapEntries)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = ((ArrayType) mapType.getValueType()).getElementType();
        RowType mapEntryType = RowType.anonymous(ImmutableList.of(keyType, valueType));

        int entryCount = mapEntries.getPositionCount();
        if (entryCount > entryIndicesList.length) {
            initializeEntryIndicesList(entryCount);
        }
        BlockSet keySet = new BlockSet(keyType, keysDistinctOperator, keyHashCode, entryCount);

        for (int i = 0; i < entryCount; i++) {
            if (mapEntries.isNull(i)) {
                clearEntryIndices(keySet.size());
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map entry cannot be null");
            }
            Block mapEntryBlock = mapEntryType.getObject(mapEntries, i);

            if (mapEntryBlock.isNull(0)) {
                clearEntryIndices(keySet.size());
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }

            if (keySet.add(mapEntryBlock, 0)) {
                entryIndicesList[keySet.size() - 1].add(i);
            }
            else {
                entryIndicesList[keySet.positionOf(mapEntryBlock, 0)].add(i);
            }
        }

        Block resultMap = mapValueBuilder.build(keySet.size(), (keyBuilder, valueBuilder) -> {
            for (int i = 0; i < keySet.size(); i++) {
                IntList indexList = entryIndicesList[i];
                keyType.appendTo(mapEntryType.getObject(mapEntries, indexList.getInt(0)), 0, keyBuilder);
                ((ArrayBlockBuilder) valueBuilder).buildEntry(elementBuilder -> {
                    for (int entryIndex : indexList) {
                        valueType.appendTo(mapEntryType.getObject(mapEntries, entryIndex), 1, elementBuilder);
                    }
                });
            }
        });
        clearEntryIndices(keySet.size());
        return resultMap;
    }

    private void clearEntryIndices(int entryCount)
    {
        verify(entryCount <= entryIndicesList.length);
        for (int i = 0; i < entryCount; i++) {
            entryIndicesList[i].clear();
        }
    }

    private void initializeEntryIndicesList(int entryCount)
    {
        entryIndicesList = new IntList[entryCount];
        for (int i = 0; i < entryIndicesList.length; i++) {
            entryIndicesList[i] = new IntArrayList();
        }
    }
}
