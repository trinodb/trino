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
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.block.BlockAssertions.createRandomDictionaryBlock;
import static io.trino.operator.InterpretedHashGenerator.createPagePrefixHashGenerator;
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
import static org.assertj.core.api.Assertions.assertThatCode;

class TestInterpretedHashGenerator
{
    private final TypeOperators typeOperators = new TypeOperators();
    private final NullSafeHashCompiler compiler = new NullSafeHashCompiler(typeOperators);

    @Test
    void testHashPosition()
    {
        List<Type> bigTypeSet = createTestingTypes(typeOperators);
        for (int typeCount : List.of(1, 500, 501, 999, 1000, 1001, 2000, 2001)) {
            List<Type> types = bigTypeSet.subList(0, typeCount);
            InterpretedHashGenerator hashGenerator = createPagePrefixHashGenerator(types, compiler);

            Block[] blocks = createRandomData(types, 10, 0.0f);
            for (int position : List.of(0, 5, 9)) {
                assertThat(hashGenerator.hashPosition(position, new Page(blocks))).isEqualTo(manualHash(types, blocks, position));

                // Convert all blocks to RunLengthEncoded and re-check result matches
                Block[] rleBlocks = new Block[blocks.length];
                for (int i = 0; i < blocks.length; i++) {
                    rleBlocks[i] = RunLengthEncodedBlock.create(blocks[i].getSingleValueBlock(position), 10);
                }
                assertThat(hashGenerator.hashPosition(position, new Page(rleBlocks))).isEqualTo(manualHash(types, blocks, position));

                // Convert all blocks to Dictionary and check result matches
                Block[] dictionaryBlocks = new Block[blocks.length];
                for (int i = 0; i < blocks.length; i++) {
                    dictionaryBlocks[i] = createRandomDictionaryBlock(blocks[i], 10);
                }
                assertThat(hashGenerator.hashPosition(position, new Page(dictionaryBlocks))).isEqualTo(manualHash(types, dictionaryBlocks, position));
            }
        }
    }

    @Test
    void testBatchedRawHashesMatchSinglePositionHashes()
    {
        testBatchedRawHashesMatchSinglePositionHashes(true);
        testBatchedRawHashesMatchSinglePositionHashes(false);
    }

    private void testBatchedRawHashesMatchSinglePositionHashes(boolean hashBlocksBatched)
    {
        List<Type> types = createTestingTypes(typeOperators);
        InterpretedHashGenerator hashGenerator = createPagePrefixHashGenerator(types, compiler);

        int positionCount = 1024;
        Block[] blocks = createRandomData(types, positionCount, 0.25f);

        long[] hashes = new long[positionCount];
        if (hashBlocksBatched) {
            hashGenerator.hashBlocksBatched(blocks, hashes, 0, positionCount);
        }
        else {
            hashGenerator.hash(new Page(blocks), 0, positionCount, hashes);
        }
        assertHashesEqual(types, blocks, hashes, hashGenerator);

        // Convert all blocks to RunLengthEncoded and re-check result matches
        Block[] rleBlocks = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            rleBlocks[i] = RunLengthEncodedBlock.create(blocks[i].getSingleValueBlock(0), positionCount);
        }
        if (hashBlocksBatched) {
            hashGenerator.hashBlocksBatched(rleBlocks, hashes, 0, positionCount);
        }
        else {
            hashGenerator.hash(new Page(rleBlocks), 0, positionCount, hashes);
        }
        assertHashesEqual(types, rleBlocks, hashes, hashGenerator);

        // Convert all blocks to Dictionary and check result matches
        Block[] dictionaryBlocks = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            // Effective dictionaries
            dictionaryBlocks[i] = createRandomDictionaryBlock(blocks[i].getRegion(0, 6), positionCount + 1);
            // Add an ids offset to the dictionary blocks for better test coverage
            dictionaryBlocks[i] = dictionaryBlocks[i].getRegion(1, positionCount);
        }
        if (hashBlocksBatched) {
            hashGenerator.hashBlocksBatched(dictionaryBlocks, hashes, 0, positionCount);
        }
        else {
            hashGenerator.hash(new Page(dictionaryBlocks), 0, positionCount, hashes);
        }
        assertHashesEqual(types, dictionaryBlocks, hashes, hashGenerator);

        for (int i = 0; i < blocks.length; i++) {
            // In-effective dictionaries
            dictionaryBlocks[i] = createRandomDictionaryBlock(blocks[i], positionCount + 1);
            // Add an ids offset to the dictionary blocks for better test coverage
            dictionaryBlocks[i] = dictionaryBlocks[i].getRegion(1, positionCount);
        }
        if (hashBlocksBatched) {
            hashGenerator.hashBlocksBatched(dictionaryBlocks, hashes, 0, positionCount);
        }
        else {
            hashGenerator.hash(new Page(dictionaryBlocks), 0, positionCount, hashes);
        }
        assertHashesEqual(types, dictionaryBlocks, hashes, hashGenerator);
    }

    @Test
    void testBatchedRawHashesZeroLength()
    {
        List<Type> types = createTestingTypes(typeOperators);
        InterpretedHashGenerator hashGenerator = createPagePrefixHashGenerator(types, compiler);

        int positionCount = 10;
        // Attempting to touch any of the blocks would result in a NullPointerException
        assertThatCode(() -> hashGenerator.hashBlocksBatched(new Block[types.size()], new long[positionCount], 0, 0))
                .doesNotThrowAnyException();
    }

    private void assertHashesEqual(List<Type> types, Block[] blocks, long[] batchedHashes, InterpretedHashGenerator hashGenerator)
    {
        for (int position = 0; position < batchedHashes.length; position++) {
            long manualRowHash = manualHash(types, blocks, position);
            long singleRowHash = hashGenerator.hashPosition(position, new Page(blocks));
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
}
