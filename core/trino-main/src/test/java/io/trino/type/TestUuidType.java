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
package io.trino.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.type.UuidOperators.castFromVarcharToUuid;
import static java.lang.Long.reverseBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestUuidType
        extends AbstractTestType
{
    public TestUuidType()
    {
        super(UUID, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = UUID.createBlockBuilder(null, 1);
        for (int i = 0; i < 10; i++) {
            String uuid = "6b5f5b65-67e4-43b0-8ee3-586cd49f58a" + i;
            UUID.writeSlice(blockBuilder, castFromVarcharToUuid(utf8Slice(uuid)));
        }
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Slice slice = (Slice) value;
        return Slices.wrappedLongArray(slice.getLong(0), reverseBytes(reverseBytes(slice.getLong(SIZE_OF_LONG)) + 1));
    }

    @Override
    protected Object getNonNullValue()
    {
        return Slices.wrappedLongArray(0, 0);
    }

    @Test
    public void testDisplayName()
    {
        assertEquals(UUID.getDisplayName(), "uuid");
    }

    @Test
    public void testJavaUuidToTrinoUuid()
    {
        assertThat(javaUuidToTrinoUuid(java.util.UUID.fromString("00000000-0000-0000-0000-000000000001")))
                .isEqualTo(castFromVarcharToUuid(utf8Slice("00000000-0000-0000-0000-000000000001")));

        assertThat(javaUuidToTrinoUuid(java.util.UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7")))
                .isEqualTo(castFromVarcharToUuid(utf8Slice("f79c3e09-677c-4bbd-a479-3f349cb785e7")));
    }

    @Test
    public void testOrdering()
            throws Throwable
    {
        String lowerAsString = "406caec7-68b9-4778-81b2-a12ece70c8b1";
        String higherAsString = "f79c3e09-677c-4bbd-a479-3f349cb785e7";
        java.util.UUID lower = java.util.UUID.fromString(lowerAsString);
        java.util.UUID higher = java.util.UUID.fromString(higherAsString);
        // Java UUID's comparison is not consitent with RFC 4122, see https://bugs.openjdk.org/browse/JDK-7025832
        assertThat(higher).isLessThan(lower);

        Slice lowerSlice = javaUuidToTrinoUuid(lower);
        Slice higherSlice = javaUuidToTrinoUuid(higher);

        MethodHandle compareByValue = new TypeOperators().getComparisonUnorderedFirstOperator(UUID, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
        long comparisonByValue = (long) compareByValue.invoke(lowerSlice, higherSlice);
        assertThat(comparisonByValue)
                .as("value comparison operator result")
                .isLessThan(0);

        MethodHandle compareFromBlock = new TypeOperators().getComparisonUnorderedFirstOperator(UUID, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        long comparisonFromBlock = (long) compareFromBlock.invoke(nativeValueToBlock(UUID, lowerSlice), 0, nativeValueToBlock(UUID, higherSlice), 0);
        assertThat(comparisonFromBlock)
                .as("block-position comparison operator result")
                .isLessThan(0);

        // UUID ordering should be consistent with lexicographical order of unsigned bytes the UUID is comprised of
        assertThat(lowerSlice)
                .as("comparing slices lexicographically")
                .isLessThan(higherSlice);
    }
}
