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
package io.trino.spiller;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.execution.buffer.PagesSerde;
import io.trino.execution.buffer.TestingPagesSerdeFactory;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.execution.buffer.PagesSerde.isSerializedPageEncrypted;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestSpillCipherPagesSerde
{
    @Test
    public void test()
    {
        SpillCipher cipher = new AesSpillCipher();
        PagesSerde serde = new TestingPagesSerdeFactory().createPagesSerdeForSpill(Optional.of(cipher));
        List<Type> types = ImmutableList.of(VARCHAR);
        Page emptyPage = new Page(VARCHAR.createBlockBuilder(null, 0).build());
        try (PagesSerde.PagesSerdeContext context = serde.newContext()) {
            assertPageEquals(types, serde.deserialize(serde.serialize(context, emptyPage)), emptyPage);

            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 2);
            VARCHAR.writeString(blockBuilder, "hello");
            VARCHAR.writeString(blockBuilder, "world");
            Page helloWorldPage = new Page(blockBuilder.build());

            Slice serialized = serde.serialize(context, helloWorldPage);
            assertPageEquals(types, serde.deserialize(serialized), helloWorldPage);
            assertTrue(isSerializedPageEncrypted(serialized), "page should be encrypted");

            cipher.close();

            assertFailure(() -> serde.serialize(context, helloWorldPage), "Spill cipher already closed");
            assertFailure(() -> serde.deserialize(context, serialized), "Spill cipher already closed");
        }
    }

    private static void assertFailure(ThrowingRunnable runnable, String expectedErrorMessage)
    {
        TrinoException exception = expectThrows(TrinoException.class, runnable);
        assertEquals(exception.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertEquals(exception.getMessage(), expectedErrorMessage);
    }
}
