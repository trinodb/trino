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
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Convention;
import io.trino.spi.function.Description;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;

@ScalarFunction("map_from_entries")
@Description("Construct a map from an array of entries")
public final class MapFromEntriesFunction
{
    private final BufferedMapValueBuilder mapValueBuilder;

    @TypeParameter("K")
    @TypeParameter("V")
    public MapFromEntriesFunction(@TypeParameter("map(K,V)") Type mapType)
    {
        mapValueBuilder = BufferedMapValueBuilder.createBufferedDistinctStrict((MapType) mapType);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("map(K,V)")
    @SqlNullable
    public Block mapFromEntries(
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) BlockPositionIsDistinctFrom keysDistinctOperator,
            @OperatorDependency(
                    operator = HASH_CODE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL)) BlockPositionHashCode keyHashCode,
            @TypeParameter("map(K,V)") MapType mapType,
            ConnectorSession session,
            @SqlType("array(row(K,V))") Block mapEntries)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        RowType mapEntryType = RowType.anonymous(ImmutableList.of(keyType, valueType));

        int entryCount = mapEntries.getPositionCount();

        try {
            return mapValueBuilder.build(entryCount, (keyBuilder, valueBuilder) -> {
                for (int i = 0; i < entryCount; i++) {
                    if (mapEntries.isNull(i)) {
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map entry cannot be null");
                    }
                    Block mapEntryBlock = mapEntryType.getObject(mapEntries, i);

                    if (mapEntryBlock.isNull(0)) {
                        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
                    }

                    keyType.appendTo(mapEntryBlock, 0, keyBuilder);
                    valueType.appendTo(mapEntryBlock, 1, valueBuilder);
                }
            });
        }
        catch (DuplicateMapKeyException e) {
            throw e.withDetailedMessage(mapType.getKeyType(), session);
        }
    }
}
