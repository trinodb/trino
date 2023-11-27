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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static io.trino.util.StructuralTestUtil.mapType;
import static io.trino.util.StructuralTestUtil.sqlMapOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArrayOfMapOfBigintVarcharType
        extends AbstractTestType
{
    private static final ArrayType TYPE = new ArrayType(mapType(BIGINT, VARCHAR));

    public TestArrayOfMapOfBigintVarcharType()
    {
        super(TYPE, List.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TYPE.createBlockBuilder(null, 4);
        TYPE.writeObject(blockBuilder, arrayBlockOf(TYPE.getElementType(),
                sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(1, "hi")),
                sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(2, "bye"))));
        TYPE.writeObject(blockBuilder, arrayBlockOf(TYPE.getElementType(),
                sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(1, "2", 2, "hello")),
                sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(3, "4", 4, "bye"))));
        TYPE.writeObject(blockBuilder, arrayBlockOf(TYPE.getElementType(),
                sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(100, "hundred")),
                sqlMapOf(BIGINT, VARCHAR, ImmutableMap.of(200, "two hundred"))));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThatThrownBy(() -> type.getPreviousValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }

    @Test
    public void testNextValue()
    {
        assertThatThrownBy(() -> type.getPreviousValue(getSampleValue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Type is not orderable: " + type);
    }
}
