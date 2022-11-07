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
package io.trino.block;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.testng.Assert.assertEquals;

public class TestSingleRowBlockWriter
{
    private RowBlockBuilder rowBlockBuilder;

    @BeforeClass
    public void setup()
    {
        List<Type> types = ImmutableList.of(BIGINT, BOOLEAN);
        rowBlockBuilder = (RowBlockBuilder) RowType.anonymous(types).createBlockBuilder(null, 8);
    }

    @Test
    public void testGetSizeInBytes()
    {
        SingleRowBlockWriter singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
        // Test whether new singleRowBlockWriter has size equal to 0
        assertEquals(0, singleRowBlockWriter.getSizeInBytes());

        singleRowBlockWriter.writeLong(10).closeEntry();
        assertEquals(9, singleRowBlockWriter.getSizeInBytes());

        singleRowBlockWriter.writeByte(10).closeEntry();
        assertEquals(11, singleRowBlockWriter.getSizeInBytes());
        rowBlockBuilder.closeEntry();

        // Test whether previous entry does not mix to the next entry (for size). Does reset works on size?
        singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
        assertEquals(0, singleRowBlockWriter.getSizeInBytes());

        singleRowBlockWriter.writeLong(10).closeEntry();
        assertEquals(9, singleRowBlockWriter.getSizeInBytes());

        singleRowBlockWriter.writeByte(10).closeEntry();
        assertEquals(11, singleRowBlockWriter.getSizeInBytes());
        rowBlockBuilder.closeEntry();
    }
}
