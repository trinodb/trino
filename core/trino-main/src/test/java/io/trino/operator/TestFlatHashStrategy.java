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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.operator.scalar.CombineHashFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestFlatHashStrategy
{
    private static final int FIXED_CHUNK_OFFSET = 11;
    private static final int VARIABLE_CHUNK_OFFSET = 17;

    private final TypeOperators typeOperators = new TypeOperators();
    private final FlatHashStrategyCompiler compiler = new FlatHashStrategyCompiler(typeOperators);

    @Test
    void test()
    {
        List<Type> bigTypeSet = createTestingTypes(typeOperators);
        for (int typeCount : List.of(1, 500, 501, 999, 1000, 1001, 2000, 2001)) {
            List<Type> types = bigTypeSet.subList(0, typeCount);
            FlatHashStrategy flatHashStrategy = compiler.getFlatHashStrategy(types);
            assertThat(flatHashStrategy.isAnyVariableWidth()).isEqualTo(types.stream().anyMatch(Type::isFlatVariableWidth));
            int flatFixedLength = flatHashStrategy.getTotalFlatFixedLength();
            assertThat(flatFixedLength).isEqualTo(types.stream().mapToInt(Type::getFlatFixedSize).sum() + types.size());

            Block[] blocks = createRandomData(types, 10, 0.0f);
            for (int position : List.of(0, 5, 9)) {
                int variableWidth = flatHashStrategy.getTotalVariableWidth(blocks, position);
                assertThat(variableWidth).isEqualTo(manualGetTotalVariableWidth(types, blocks, position));
                assertThat(flatHashStrategy.hash(blocks, position)).isEqualTo(manualHash(types, blocks, position));

                byte[] fixedChunk = new byte[flatFixedLength + FIXED_CHUNK_OFFSET];
                byte[] variableChunk = new byte[variableWidth + VARIABLE_CHUNK_OFFSET];
                flatHashStrategy.writeFlat(blocks, position, fixedChunk, FIXED_CHUNK_OFFSET, variableChunk, VARIABLE_CHUNK_OFFSET);
                assertThat(fixedChunk).startsWith(new byte[FIXED_CHUNK_OFFSET]);
                assertThat(variableChunk).startsWith(new byte[VARIABLE_CHUNK_OFFSET]);

                assertThat(flatHashStrategy.hash(fixedChunk, FIXED_CHUNK_OFFSET, variableChunk)).isEqualTo(manualHash(types, blocks, position));
                assertThat(flatHashStrategy.valueNotDistinctFrom(fixedChunk, FIXED_CHUNK_OFFSET, variableChunk, blocks, position)).isTrue();
                assertThat(flatHashStrategy.valueNotDistinctFrom(fixedChunk, FIXED_CHUNK_OFFSET, variableChunk, blocks, 3)).isFalse();

                BlockBuilder[] blockBuilders = types.stream().map(type -> type.createBlockBuilder(null, 1)).toArray(BlockBuilder[]::new);
                flatHashStrategy.readFlat(fixedChunk, FIXED_CHUNK_OFFSET, variableChunk, blockBuilders);
                List<Block> output = Arrays.stream(blockBuilders).map(BlockBuilder::build).toList();
                Page actualPage = new Page(output.toArray(Block[]::new));
                Page expectedPage = new Page(blocks).getSingleValuePage(position);
                assertPageEquals(types, actualPage, expectedPage);
            }
        }
    }

    @Test
    void testBatchedRawHashesZeroLength()
    {
        List<Type> types = createTestingTypes(typeOperators);
        FlatHashStrategy flatHashStrategy = compiler.getFlatHashStrategy(types);

        int positionCount = 10;
        // Attempting to touch any of the blocks would result in a NullPointerException
        assertDoesNotThrow(() -> flatHashStrategy.hashBlocksBatched(new Block[types.size()], new long[positionCount], 0, 0));
    }

    @Test
    void testBatchedRawHashesMatchSinglePositionHashes()
    {
        List<Type> types = createTestingTypes(typeOperators);
        FlatHashStrategy flatHashStrategy = compiler.getFlatHashStrategy(types);

        int positionCount = 1024;
        Block[] blocks = createRandomData(types, positionCount, 0.25f);

        long[] hashes = new long[positionCount];
        flatHashStrategy.hashBlocksBatched(blocks, hashes, 0, positionCount);
        assertHashesEqual(types, blocks, hashes, flatHashStrategy);

        // Convert all blocks to RunLengthEncoded and re-check results match
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = RunLengthEncodedBlock.create(blocks[i].getSingleValueBlock(0), positionCount);
        }
        flatHashStrategy.hashBlocksBatched(blocks, hashes, 0, positionCount);
        assertHashesEqual(types, blocks, hashes, flatHashStrategy);

        // Ensure the formatting logic produces a real string and doesn't blow up since otherwise this code wouldn't be exercised
        assertNotNull(singleRowTypesAndValues(types, blocks, 0));
    }

    private void assertHashesEqual(List<Type> types, Block[] blocks, long[] batchedHashes, FlatHashStrategy flatHashStrategy)
    {
        for (int position = 0; position < batchedHashes.length; position++) {
            long manualRowHash = manualHash(types, blocks, position);
            long singleRowHash = flatHashStrategy.hash(blocks, position);
            assertThat(singleRowHash).isEqualTo(manualRowHash);
            assertThat(singleRowHash).isEqualTo(batchedHashes[position]);
        }
    }

    private static Block[] createRandomData(List<Type> types, int positionCount, float nullRate)
    {
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = createRandomBlockForType(types.get(i), positionCount, nullRate);
        }
        return blocks;
    }

    private static long manualGetTotalVariableWidth(List<Type> types, Block[] blocks, int position)
    {
        long totalVariableWidth = 0;
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Block block = blocks[i];
            if (type.isFlatVariableWidth()) {
                if (!block.isNull(position)) {
                    totalVariableWidth += type.getFlatVariableWidthSize(block, position);
                }
            }
        }
        return totalVariableWidth;
    }

    private long manualHash(List<Type> types, Block[] blocks, int position)
    {
        long manualRowHash = 0;
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Block block = blocks[i];
            try {
                long fieldHash = 0;
                if (!block.isNull(position)) {
                    fieldHash = (long) typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)).invoke(block, position);
                }
                manualRowHash = CombineHashFunction.getHash(manualRowHash, fieldHash);
            }
            catch (Throwable e) {
                throw new RuntimeException("Error hashing field " + type, e);
            }
        }
        return manualRowHash;
    }

    private static List<Type> createTestingTypes(TypeOperators typeOperators)
    {
        List<Type> baseTypes = List.of(
                BIGINT,
                BOOLEAN,
                createCharType(5),
                createDecimalType(18),
                createDecimalType(38),
                DOUBLE,
                INTEGER,
                IPADDRESS,
                REAL,
                TIMESTAMP_SECONDS,
                TIMESTAMP_MILLIS,
                TIMESTAMP_MICROS,
                TIMESTAMP_NANOS,
                TIMESTAMP_PICOS,
                UUID,
                VARBINARY,
                VARCHAR);

        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        builder.addAll(baseTypes);
        builder.add(RowType.anonymous(baseTypes));
        for (Type baseType : baseTypes) {
            builder.add(new ArrayType(baseType));
            builder.add(new MapType(baseType, baseType, typeOperators));
        }
        return nCopies(500, builder.build()).stream().flatMap(List::stream).limit(2001).toList();
    }

    private static String singleRowTypesAndValues(List<Type> types, Block[] blocks, int position)
    {
        ConnectorSession connectorSession = TestingSession.testSessionBuilder().build().toConnectorSession();
        StringBuilder builder = new StringBuilder();
        int column = 0;
        for (Type type : types) {
            builder.append("\n\t");
            builder.append(type);
            builder.append(": ");
            builder.append(type.getObjectValue(connectorSession, blocks[column], position));
            column++;
        }
        return builder.toString();
    }
}
