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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.nio.charset.Charset;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestRowBlockEncoding
        extends BaseBlockEncodingTest<Object[]>
{
    @Override
    protected Type getType()
    {
        return RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR));
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Object[] value)
    {
        BlockBuilder row = blockBuilder.beginBlockEntry();
        BIGINT.writeLong(row, (long) value[0]);
        VARCHAR.writeSlice(row, utf8Slice((String) value[1]));
        blockBuilder.closeEntry();
    }

    @Override
    protected Object[] randomValue(Random random)
    {
        byte[] data = new byte[random.nextInt(256)];
        random.nextBytes(data);
        return new Object[] {random.nextLong(), new String(data, Charset.defaultCharset()), null};
    }
}
